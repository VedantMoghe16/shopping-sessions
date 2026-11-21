#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace shopping {
namespace fs = std::filesystem;
using Text = std::string_view;
using Key = uint64_t;
using Time = int64_t;

constexpr Time day_ms = 86400000;
constexpr Time week_ms = 7 * day_ms;
constexpr Time bad_time = std::numeric_limits<Time>::min();

inline uint64_t hash_key(uint64_t key) {
    key ^= key >> 30;
    key *= UINT64_C(0xbf58476d1ce4e5b9);
    key ^= key >> 27;
    key *= UINT64_C(0x94d049bb133111eb);
    return key ^ (key >> 31);
}

struct KeyHash {
    size_t operator()(Key key) const { return hash_key(key); }
};

template<class Value>
using KeyMap = std::unordered_map<Key, Value, KeyHash>;

class Keys {
    static constexpr Key wide = Key{1} << 63;
    std::mutex mutex_;
    std::unordered_map<Text, Key> indices_;
    std::vector<Text> strings_;

    static Text decode(Key key, char (&buffer)[9]) {
        size_t size = 0;
        while (key != 0) {
            buffer[size++] = static_cast<char>((key >> 56) & 127);
            key = (key << 7) & ~wide;
        }
        return Text(buffer, size);
    }

public:
    Key encode(Text text) {
        if (text.size() <= 9) {
            Key key = 0;
            size_t index = 0;
            for (unsigned char byte : text) {
                if (byte == 0 || byte > 127) break;
                key = (key << 7) | byte;
                ++index;
            }
            if (index == text.size()) {
                return index == 0 ? 0 : key << (63 - index * 7);
            }
        }
        std::lock_guard<std::mutex> lock(mutex_);
        auto entry = indices_.try_emplace(text, wide | strings_.size());
        if (entry.second) strings_.push_back(text);
        return entry.first->second;
    }

    Text text(Key key, char (&buffer)[9]) {
        if ((key & wide) == 0) return decode(key, buffer);
        std::lock_guard<std::mutex> lock(mutex_);
        return strings_[key & ~wide];
    }

    bool less(Key a, Key b) {
        if (((a | b) & wide) == 0) return a < b;
        char a_buffer[9], b_buffer[9];
        return text(a, a_buffer).compare(text(b, b_buffer)) < 0;
    }
};

class MappedFile {
    const char* data_ = nullptr;
    size_t size_ = 0;

public:
    explicit MappedFile(const fs::path& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("open " + path.string());
        struct stat info {};
        if (fstat(fd, &info) != 0) {
            close(fd);
            throw std::runtime_error("stat " + path.string());
        }
        size_ = static_cast<size_t>(info.st_size);
        if (size_ != 0) {
            void* region = mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd, 0);
            if (region == MAP_FAILED) {
                close(fd);
                throw std::runtime_error("mmap " + path.string());
            }
            data_ = static_cast<const char*>(region);
        }
        close(fd);
    }

    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;
    ~MappedFile() { if (data_) munmap(const_cast<char*>(data_), size_); }

    Text bytes() const { return size_ ? Text(data_, size_) : Text(); }
};

inline size_t next_line(Text bytes, size_t pos) {
    const size_t newline = bytes.find('\n', pos);
    return newline == Text::npos ? bytes.size() : newline + 1;
}

template<class Visit>
void lines(Text bytes, Visit visit) {
    while (!bytes.empty()) {
        const size_t newline = bytes.find('\n');
        if (newline == Text::npos) {
            visit(bytes);
            break;
        }
        visit(bytes.substr(0, newline));
        bytes.remove_prefix(newline + 1);
    }
}

template<size_t N>
bool fields(Text row, std::array<Text, N>& output) {
    for (size_t i = 0; i < N; ++i) {
        const size_t comma = row.find(',');
        if ((comma == Text::npos) != (i == N - 1)) return false;
        Text field = row.substr(0, comma);
        if (!field.empty() && field.front() == '"') {
            if (field.size() < 2 || field.back() != '"') return false;
            field.remove_prefix(1);
            field.remove_suffix(1);
        }
        if (field.find('"') != Text::npos) return false;
        output[i] = field;
        if (i + 1 < N) row.remove_prefix(comma + 1);
    }
    return true;
}

inline Time digits(Text value, Time limit) {
    if (value.empty()) return bad_time;
    Time result = 0;
    const Time max_prefix = limit / 10;
    const int max_last = limit % 10;
    for (unsigned char byte : value) {
        const unsigned digit = byte - static_cast<unsigned>('0');
        if (digit > 9 || result > max_prefix ||
            (result == max_prefix && digit > static_cast<unsigned>(max_last))) return bad_time;
        result = result * 10 + digit;
    }
    return result;
}

inline int fixed_digits(const char* text, size_t length) {
    int number = 0;
    for (size_t i = 0; i < length; ++i) {
        const unsigned digit = static_cast<unsigned char>(text[i]) - static_cast<unsigned>('0');
        if (digit > 9) return -1;
        number = number * 10 + static_cast<int>(digit);
    }
    return number;
}

inline Time civil_days(int year, int month, int day) {
    year -= month <= 2;
    const int era = (year >= 0 ? year : year - 399) / 400;
    const int y = year - era * 400;
    const int m = month + (month > 2 ? -3 : 9);
    return era * 146097 + y * 365 + y / 4 - y / 100 + (153 * m + 2) / 5 + day - 1 - 719468;
}

inline Time timestamp(Text value, bool utc_only = false) {
    const size_t size = value.size();
    const bool f1 = size == 20 && value[19] == 'Z';
    const bool f2 = !utc_only && size == 25 && (value[19] == '+' || value[19] == '-');
    if (!f1 && !f2) {
        return utc_only ? bad_time : digits(value, std::numeric_limits<Time>::max());
    }
    if (value[4] != '-' || value[7] != '-' || value[10] != 'T' ||
        value[13] != ':' || value[16] != ':') return bad_time;
    const char* p = value.data();
    const int year = fixed_digits(p, 4), month = fixed_digits(p + 5, 2);
    const int day = fixed_digits(p + 8, 2), hour = fixed_digits(p + 11, 2);
    const int minute = fixed_digits(p + 14, 2), second = fixed_digits(p + 17, 2);
    if (year < 0 || month < 1 || month > 12 || day < 1 ||
        hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) return bad_time;
    static constexpr int month_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    const bool leap = month == 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    if (day > month_days[month - 1] + leap) return bad_time;
    int offset = 0;
    if (f2) {
        const int h = fixed_digits(p + 20, 2), m = fixed_digits(p + 23, 2);
        if (p[22] != ':' || h < 0 || h > 23 || m < 0 || m > 59) return bad_time;
        offset = (h * 60 + m) * (p[19] == '-' ? -1 : 1);
    }
    return civil_days(year, month, day) * day_ms + ((hour * 60 + minute - offset) * 60 + second) * Time{1000};
}

inline int32_t epoch_day(Time time) {
    return static_cast<int32_t>(time / day_ms - (time % day_ms < 0));
}
}
