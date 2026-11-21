#pragma once

#include "input.hpp"
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>

namespace shopping {
constexpr size_t batch_size = 65536;

inline void check_arrow(const arrow::Status& status) {
    if (!status.ok()) throw std::runtime_error(status.ToString());
}

template<class Buffer>
std::shared_ptr<arrow::Buffer> view_arrow(const Buffer& buffer) {
    return std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(buffer.data()),
                                           buffer.size() * sizeof(buffer[0]));
}

struct Strings {
    std::vector<uint32_t> offsets{0};
    std::string bytes;
    std::vector<uint8_t> valid;

    void add(Text value, bool present = true) {
        const size_t index = offsets.size() - 1;
        if (index % 8 == 0) valid.push_back(0);
        if (present) {
            valid.back() |= 1u << (index % 8);
            bytes.append(value);
        }
        if (bytes.size() > INT32_MAX) throw std::runtime_error("string batch exceeds Arrow offset range");
        offsets.push_back(static_cast<uint32_t>(bytes.size()));
    }
    void clear() {
        offsets.resize(1);
        bytes.clear();
        valid.clear();
    }

    std::shared_ptr<arrow::Array> array(bool nullable) const {
        return arrow::MakeArray(arrow::ArrayData::Make(arrow::utf8(), offsets.size() - 1,
            {nullable ? view_arrow(valid) : nullptr, view_arrow(offsets), view_arrow(bytes)}));
    }
};

template<size_t Count>
class BatchWriter {
    std::shared_ptr<arrow::io::FileOutputStream> file_;
    std::unique_ptr<parquet::arrow::FileWriter> writer_;
    Strings first_, second_;
    std::vector<int32_t> days_;
    std::array<std::vector<int64_t>, Count> numbers_;

    static const arrow::Schema& schema() {
        static const auto schema = [] {
            std::vector<std::shared_ptr<arrow::Field>> fields;
            fields.push_back(arrow::field(Count == 3 ? "user_id" : "campaign_id", arrow::utf8()));
            if constexpr (Count == 4) fields.push_back(arrow::field("channel", arrow::utf8()));
            fields.push_back(arrow::field("day", arrow::date32()));
            fields.push_back(arrow::field("sessions", arrow::int64()));
            fields.push_back(arrow::field(Count == 3 ? "events" : "purchasing_sessions", arrow::int64()));
            fields.push_back(arrow::field("revenue_cents", arrow::int64()));
            if constexpr (Count == 3) fields.push_back(arrow::field("top_category", arrow::utf8()));
            else fields.push_back(arrow::field("assisted_sessions", arrow::int64()));
            return arrow::schema(std::move(fields));
        }();
        return *schema;
    }

public:
    explicit BatchWriter(const fs::path& path) {
        auto file = arrow::io::FileOutputStream::Open(path.string());
        check_arrow(file.status());
        file_ = std::move(file).ValueOrDie();
        static const auto properties = parquet::WriterProperties::Builder()
            .compression(parquet::Compression::SNAPPY)->build();
        static const auto arrow_properties = parquet::ArrowWriterProperties::Builder()
            .set_use_threads(false)->build();
        auto writer = parquet::arrow::FileWriter::Open(schema(), arrow::default_memory_pool(),
                                                       file_, properties, arrow_properties);
        check_arrow(writer.status());
        writer_ = std::move(writer).ValueOrDie();
    }

    void add(Text first, Text second, bool second_present, int32_t day,
             const std::array<int64_t, Count>& numbers) {
        first_.add(first);
        second_.add(second, second_present);
        days_.push_back(day);
        for (size_t i = 0; i < Count; ++i) numbers_[i].push_back(numbers[i]);
        if (days_.size() >= batch_size) flush();
    }

    void flush() {
        if (days_.empty()) return;
        check_arrow(writer_->NewRowGroup());
        const auto write = [&](const std::shared_ptr<arrow::Array>& data) {
            check_arrow(writer_->WriteColumnChunk(*data));
        };
        write(first_.array(false));
        if constexpr (Count == 4) write(second_.array(false));
        write(arrow::MakeArray(arrow::ArrayData::Make(arrow::date32(), days_.size(),
                                                     {nullptr, view_arrow(days_)})));
        for (auto& values : numbers_) {
            write(arrow::MakeArray(arrow::ArrayData::Make(arrow::int64(), values.size(),
                                                         {nullptr, view_arrow(values)})));
            values.clear();
        }
        if constexpr (Count == 3) write(second_.array(true));
        first_.clear();
        second_.clear();
        days_.clear();
    }

    void close() {
        flush();
        check_arrow(writer_->Close());
        check_arrow(file_->Close());
    }
};
}
