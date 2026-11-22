#pragma once

#include "input.hpp"

#include <fstream>
#include <memory>
#include <utility>

#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__
#error "This Parquet writer requires a little-endian host"
#endif

namespace shopping {
namespace flat_parquet {
// A flat DataPage V1 writer for the two output schemas.
enum class Wire : uint8_t { i32 = 5, i64 = 6, binary = 8, list = 9, structure = 12 };
enum class Physical { int32 = 1, int64 = 2, bytes = 6 };
enum class Logical { none = -1, string = 0, date = 6 };
constexpr uint32_t plain = 0;
constexpr uint32_t rle = 3;
constexpr uint32_t rle_dictionary = 8;
constexpr uint32_t codec = 0;

struct Column {
    Text name;
    Physical type;
    Logical logical = Logical::none;
    bool nullable = false;
};

inline void varint(std::string& bytes, uint64_t value) {
    while (value >= 128) {
        bytes.push_back(static_cast<char>((value & 127) | 128));
        value >>= 7;
    }
    bytes.push_back(static_cast<char>(value));
}

inline void little_u32(std::string& bytes, uint32_t value) {
    bytes.append(reinterpret_cast<const char*>(&value), sizeof(value));
}

class Compact {
    std::string& bytes_;
    uint8_t last_ = 0;

    void field(uint8_t id, Wire wire) {
        // All emitted struct fields are ascending with gaps below 16.
        const uint8_t delta = id - last_;
        if (delta == 0 || delta > 15) throw std::runtime_error("unsupported Thrift field delta");
        bytes_.push_back(static_cast<char>((delta << 4) | static_cast<uint8_t>(wire)));
        last_ = id;
    }

public:
    explicit Compact(std::string& bytes) : bytes_(bytes) {}
    void number(uint8_t id, Wire wire, uint64_t value) {
        field(id, wire);
        varint(bytes_, value << 1);  // All written metadata integers are nonnegative.
    }
    static void binary_value(std::string& bytes, Text value) {
        varint(bytes, value.size());
        bytes.append(value);
    }
    void binary(uint8_t id, Text value) {
        field(id, Wire::binary);
        binary_value(bytes_, value);
    }
    void list(uint8_t id, Wire element, size_t count) {
        field(id, Wire::list);
        bytes_.push_back(static_cast<char>((std::min(count, size_t{15}) << 4) |
                                          static_cast<uint8_t>(element)));
        if (count >= 15) varint(bytes_, count);
    }
    void structure(uint8_t id) { field(id, Wire::structure); }
    void end() { bytes_.push_back(0); }
};

inline void schema_leaf(std::string& footer, Column column) {
    Compact node(footer);
    node.number(1, Wire::i32, static_cast<int>(column.type));
    node.number(3, Wire::i32, column.nullable);
    node.binary(4, column.name);
    if (column.logical != Logical::none) {
        node.number(6, Wire::i32, static_cast<int>(column.logical));
        node.structure(10);
        Compact logical(footer);
        logical.structure(column.logical == Logical::string ? 1 : 6);
        Compact(footer).end();
        logical.end();
    }
    node.end();
}

class Strings {
    std::unique_ptr<char[]> bytes_;
    size_t size_ = 0, capacity_ = 0;

public:
    std::vector<uint8_t> valid;

    Text values() const { return size_ ? Text(bytes_.get(), size_) : Text{}; }

    void append(Text value) {
        if (value.size() > INT32_MAX) throw std::runtime_error("Parquet string too long");
        const auto length = static_cast<uint32_t>(value.size());
        const size_t next = size_ + sizeof(length) + length;
        if (next > INT32_MAX) throw std::runtime_error("Parquet string page too large");
        if (next > capacity_) {
            const size_t reserve = std::max(next, std::max(size_t{64}, capacity_ * 2));
            std::unique_ptr<char[]> storage(new char[reserve]);
            if (size_) std::memcpy(storage.get(), bytes_.get(), size_);
            bytes_ = std::move(storage);
            capacity_ = reserve;
        }
        char* destination = bytes_.get() + size_;
        std::memcpy(destination, &length, sizeof(length));
        if (length) std::memcpy(destination + sizeof(length), value.data(), length);
        size_ = next;
    }

    void append_optional(Text value, bool present, size_t row) {
        if ((row & 7) == 0) valid.push_back(0);
        if (!present) return;
        valid.back() |= 1u << (row & 7);
        append(value);
    }

    void clear() {
        size_ = 0;
        valid.clear();
    }
};
}

template<size_t Count>
class BatchWriter {
    using Column = flat_parquet::Column;
    using Physical = flat_parquet::Physical;
    using Logical = flat_parquet::Logical;
    using Compact = flat_parquet::Compact;
    using Wire = flat_parquet::Wire;
    static constexpr size_t batch_size = 65536;
    static constexpr size_t columns = Count + 3;
    static_assert(Count == 3 || Count == 4, "unsupported output schema");

    static constexpr std::array<Column, columns> schema() {
        if constexpr (Count == 3) {
            return {{{"user_id", Physical::bytes, Logical::string},
                     {"day", Physical::int32, Logical::date},
                     {"sessions", Physical::int64}, {"events", Physical::int64},
                     {"revenue_cents", Physical::int64},
                     {"top_category", Physical::bytes, Logical::string, true}}};
        } else {
            return {{{"campaign_id", Physical::bytes, Logical::string},
                     {"channel", Physical::bytes, Logical::string},
                     {"day", Physical::int32, Logical::date}, {"sessions", Physical::int64},
                     {"purchasing_sessions", Physical::int64}, {"revenue_cents", Physical::int64},
                     {"assisted_sessions", Physical::int64}}};
        }
    }

    struct Chunk {
        uint64_t offset, bytes;
        uint64_t dictionary_offset = 0;
    };
    struct Group {
        std::array<Chunk, columns> chunks;
        size_t rows;
    };

    std::ofstream file_;
    uint64_t offset_ = 0, rows_ = 0;
    std::vector<Group> groups_;
    flat_parquet::Strings first_, second_;
    std::vector<int32_t> days_;
    std::array<std::vector<int64_t>, Count> numbers_;
    std::string header_, nullable_, user_runs_;
    Key last_user_ = 0;
    uint32_t user_count_ = 0, user_run_ = 0;

    void write(Text bytes) {
        if (!bytes.empty()) file_.write(bytes.data(), bytes.size());
        offset_ += bytes.size();
    }

    template<class Integer>
    static Text integer_bytes(const std::vector<Integer>& values) {
        return Text(reinterpret_cast<const char*>(values.data()), values.size() * sizeof(Integer));
    }

    Chunk page(Text payload, uint32_t encoding = flat_parquet::plain) {
        if (payload.size() > INT32_MAX)
            throw std::runtime_error("Parquet page too large");
        header_.clear();
        Compact header(header_);
        header.number(1, Wire::i32, 0);  // DATA_PAGE
        header.number(2, Wire::i32, payload.size());
        header.number(3, Wire::i32, payload.size());
        header.structure(5);
        Compact data_page(header_);
        data_page.number(1, Wire::i32, days_.size());
        data_page.number(2, Wire::i32, encoding);
        data_page.number(3, Wire::i32, flat_parquet::rle);
        data_page.number(4, Wire::i32, flat_parquet::rle);
        data_page.end();
        header.end();
        const Chunk chunk{offset_, header_.size() + payload.size()};
        write(header_);
        write(payload);
        return chunk;
    }

    void end_user_run() {
        if (user_run_ == 0) return;
        flat_parquet::varint(user_runs_, static_cast<uint64_t>(user_run_) << 1);
        const auto index = static_cast<uint16_t>(user_count_ - 1);
        user_runs_.append(reinterpret_cast<const char*>(&index), sizeof(index));
        user_run_ = 0;
    }

    Chunk user_dictionary() {
        const Text dictionary = first_.values();
        if (user_count_ == days_.size()) {
            user_count_ = user_run_ = 0;
            user_runs_.clear();
            return page(dictionary);
        }
        end_user_run();
        const uint64_t dictionary_offset = offset_;
        header_.clear();
        Compact header(header_);
        header.number(1, Wire::i32, 2);  // DICTIONARY_PAGE
        header.number(2, Wire::i32, dictionary.size());
        header.number(3, Wire::i32, dictionary.size());
        header.structure(7);
        Compact items(header_);
        items.number(1, Wire::i32, user_count_);
        items.number(2, Wire::i32, flat_parquet::plain);
        items.end();
        header.end();
        write(header_);
        write(dictionary);

        Chunk chunk = page(user_runs_, flat_parquet::rle_dictionary);
        chunk.bytes += chunk.offset - dictionary_offset;
        chunk.dictionary_offset = dictionary_offset;
        user_runs_.clear();
        user_count_ = 0;
        return chunk;
    }

    Text optional_strings() {
        // One hybrid bit-packed run of 1-bit definitions, padded to a whole byte.
        nullable_.clear();
        flat_parquet::varint(nullable_, (second_.valid.size() << 1) | 1);
        const uint32_t levels_size = nullable_.size() + second_.valid.size();
        nullable_.insert(0, reinterpret_cast<const char*>(&levels_size), sizeof(levels_size));
        nullable_.append(reinterpret_cast<const char*>(second_.valid.data()), second_.valid.size());
        nullable_.append(second_.values());
        return nullable_;
    }

    void footer() {
        std::string bytes;
        constexpr auto fields = schema();
        Compact metadata(bytes);
        metadata.number(1, Wire::i32, 1);
        metadata.list(2, Wire::structure, columns + 1);
        Compact root(bytes);
        root.binary(4, "schema");
        root.number(5, Wire::i32, columns);
        root.end();
        for (const Column& column : fields) flat_parquet::schema_leaf(bytes, column);
        metadata.number(3, Wire::i64, rows_);
        metadata.list(4, Wire::structure, groups_.size());
        for (const Group& group : groups_) {
            Compact row_group(bytes);
            row_group.list(1, Wire::structure, columns);
            uint64_t group_bytes = 0;
            for (size_t i = 0; i < columns; ++i) {
                const Chunk& chunk = group.chunks[i];
                const Column& column = fields[i];
                Compact column_chunk(bytes);
                column_chunk.number(2, Wire::i64, 0);
                column_chunk.structure(3);
                Compact column_metadata(bytes);
                column_metadata.number(1, Wire::i32, static_cast<int>(column.type));
                const bool dictionary = chunk.dictionary_offset != 0;
                column_metadata.list(2, Wire::i32, column.nullable || dictionary ? 2 : 1);
                flat_parquet::varint(bytes, flat_parquet::plain << 1);
                if (column.nullable) flat_parquet::varint(bytes, flat_parquet::rle << 1);
                if (dictionary) flat_parquet::varint(bytes, flat_parquet::rle_dictionary << 1);
                column_metadata.list(3, Wire::binary, 1);
                Compact::binary_value(bytes, column.name);
                column_metadata.number(4, Wire::i32, flat_parquet::codec);
                column_metadata.number(5, Wire::i64, group.rows);
                column_metadata.number(6, Wire::i64, chunk.bytes);
                column_metadata.number(7, Wire::i64, chunk.bytes);
                column_metadata.number(9, Wire::i64, chunk.offset);
                if (dictionary) column_metadata.number(11, Wire::i64, chunk.dictionary_offset);
                column_metadata.end();
                column_chunk.end();
                group_bytes += chunk.bytes;
            }
            row_group.number(2, Wire::i64, group_bytes);
            row_group.number(3, Wire::i64, group.rows);
            row_group.number(6, Wire::i64, group_bytes);
            row_group.end();
        }
        metadata.binary(6, "shopping-sessions");
        metadata.end();
        if (bytes.size() > UINT32_MAX) throw std::runtime_error("Parquet footer too large");
        const uint32_t length = bytes.size();
        flat_parquet::little_u32(bytes, length);
        bytes.append("PAR1");
        write(bytes);
    }

public:
    explicit BatchWriter(const fs::path& path) {
        file_.exceptions(std::ios::badbit | std::ios::failbit);
        file_.open(path, std::ios::binary);
        write("PAR1");
    }

    void add(Text first, Text second, bool second_present, int32_t day,
             const std::array<int64_t, Count>& numbers) {
        first_.append(first);
        add_row(second, second_present, day, numbers);
    }

    void add_user(Key user, Text name, Text category, bool category_present, int32_t day,
                  const std::array<int64_t, Count>& numbers) {
        static_assert(Count == 3);
        // Each shopper's rows are contiguous. Reset the dictionary on every row-group boundary.
        if (user_count_ == 0) user_runs_.push_back(16);
        if (user_count_ == 0 || user != last_user_) {
            end_user_run();
            first_.append(name);
            last_user_ = user;
            ++user_count_;
        }
        ++user_run_;
        add_row(category, category_present, day, numbers);
    }

private:
    void add_row(Text second, bool second_present, int32_t day,
                 const std::array<int64_t, Count>& numbers) {
        if constexpr (Count == 3) second_.append_optional(second, second_present, days_.size());
        else second_.append(second);
        days_.push_back(day);
        for (size_t i = 0; i < Count; ++i) numbers_[i].push_back(numbers[i]);
        if (days_.size() >= batch_size) flush();
    }

    void flush() {
        if (days_.empty()) return;
        Group group{{}, days_.size()};
        size_t col = 0;
        if constexpr (Count == 3) group.chunks[col++] = user_dictionary();
        else group.chunks[col++] = page(first_.values());
        if constexpr (Count == 4) group.chunks[col++] = page(second_.values());
        group.chunks[col++] = page(integer_bytes(days_));
        for (auto& values : numbers_) {
            group.chunks[col++] = page(integer_bytes(values));
            values.clear();
        }
        if constexpr (Count == 3) group.chunks[col++] = page(optional_strings());
        first_.clear();
        second_.clear();
        days_.clear();
        groups_.push_back(group);
        rows_ += group.rows;
    }

public:
    void close() {
        flush();
        footer();
        file_.close();
    }
};
}
