#pragma once

#include "input.hpp"
#include <fstream>

namespace shopping {
constexpr size_t batch_size = 65536;

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
};

template<size_t Count>
class BatchWriter {
    std::ofstream file_;
    Strings first_, second_;
    std::vector<int32_t> days_;
    std::array<std::vector<int64_t>, Count> numbers_;

    template<class Buffer>
    void write(const Buffer& buffer) {
        if (!buffer.empty()) file_.write(reinterpret_cast<const char*>(buffer.data()),
            static_cast<std::streamsize>(buffer.size() * sizeof(buffer[0])));
    }

public:
    explicit BatchWriter(const fs::path& path) {
        file_.exceptions(std::ios::badbit | std::ios::failbit);
        file_.open(path, std::ios::binary);
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
        write(std::array<uint32_t, 1>{static_cast<uint32_t>(days_.size())});
        write(first_.offsets);
        write(first_.bytes);
        write(second_.valid);
        write(second_.offsets);
        write(second_.bytes);
        write(days_);
        for (auto& values : numbers_) {
            write(values);
            values.clear();
        }
        first_.clear();
        second_.clear();
        days_.clear();
    }

    void close() {
        flush();
        file_.close();
    }
};
}
