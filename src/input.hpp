#pragma once

#include <deque>
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

#if defined(__AVX2__) || defined(__BMI2__)
#include <immintrin.h>
#endif

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

inline Key unmix_key(uint64_t key) {
    key ^= (key >> 31) ^ (key >> 62);
    key *= UINT64_C(0x319642b2d24d8ec3);
    key ^= (key >> 27) ^ (key >> 54);
    key *= UINT64_C(0x96de1b173f119089);
    return key ^ (key >> 30) ^ (key >> 60);
}

struct KeyHash {
    size_t operator()(Key key) const { return hash_key(key); }
};

template<class Value>
using KeyMap = std::unordered_map<Key, Value, KeyHash>;

class Keys {
    static constexpr Key wide = Key{1} << 63;
    static constexpr Key raw_suffix = wide | (Key{1} << 55);
    static constexpr Key user_prefix = Key{1} << 47;
    static constexpr Key byte_highs = UINT64_C(0x8080808080808080);
    std::mutex mutex_;
    std::deque<std::string> strings_;
    std::unordered_map<Text, Key> indices_;

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
        if (text.size() == 9 && (text.front() == 'e' || text.front() == 'u')) {
            Key suffix;
            std::memcpy(&suffix, text.data() + 1, 8);
            if ((suffix & byte_highs) == 0) {
                // ASCII leaves spare high bits in the eight bytes.
                return suffix | raw_suffix | (text.front() == 'u' ? user_prefix : 0);
            }
        }
#ifdef __BMI2__
        if (text.size() == 9) {
            uint64_t leading;
            std::memcpy(&leading, text.data(), 8);
            const auto last = static_cast<unsigned char>(text[8]);
            constexpr uint64_t highs = UINT64_C(0x8080808080808080);
            constexpr uint64_t ones = UINT64_C(0x0101010101010101);
            if (((leading | (leading - ones)) & highs) == 0 && last != 0 && last < 128) {
                return (_pext_u64(__builtin_bswap64(leading), ~highs) << 7) | last;
            }
        }
#endif
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
        const auto prior = indices_.find(text);
        if (prior != indices_.end()) return prior->second;
        if (strings_.size() >= (raw_suffix ^ wide)) throw std::runtime_error("too many wide IDs");
        const Key key = wide | strings_.size();
        strings_.emplace_back(text);
        indices_.emplace(Text(strings_.back()), key);
        return key;
    }

    Text text(Key key, char (&buffer)[9]) {
        if ((key & raw_suffix) == raw_suffix) {
            buffer[0] = (key & user_prefix) ? 'u' : 'e';
            const Key suffix = key & ~(raw_suffix | user_prefix);
            std::memcpy(buffer + 1, &suffix, 8);
            return Text(buffer, 9);
        }
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

#ifdef __AVX2__
using DelimiterMask = __uint128_t;

inline size_t first_bit(DelimiterMask mask) {
    const auto low = static_cast<uint64_t>(mask);
    return low ? __builtin_ctzll(low) : 64 + __builtin_ctzll(static_cast<uint64_t>(mask >> 64));
}

template<size_t N>
bool fields_simd(Text row, std::array<Text, N>& output) {
    const auto comma_bytes = _mm256_set1_epi8(',');
    const auto quote_bytes = _mm256_set1_epi8('"');
    DelimiterMask commas = 0, quotes = 0;
    size_t position = 0;
    for (; position < row.size(); position += 32) {
        const size_t load_at = std::min(position, row.size() - 32);
        const unsigned skip = position - load_at;
        const auto block = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row.data() + load_at));
        const uint32_t comma_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(block, comma_bytes));
        const uint32_t quote_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(block, quote_bytes));
        commas |= DelimiterMask(comma_mask >> skip) << position;
        quotes |= DelimiterMask(quote_mask >> skip) << position;
    }
    size_t begin = 0;
    unsigned outer_quotes = 0;
    for (size_t i = 0; i < N; ++i) {
        if ((commas == 0) != (i == N - 1)) return false;
        const size_t end = commas ? first_bit(commas) : row.size();
        commas &= commas - 1;
        const size_t length = end - begin;
        const size_t trim = length >= 2 && row[begin] == '"' && row[end - 1] == '"';
        outer_quotes += 2 * trim;
        output[i] = Text(row.data() + begin + trim, length - 2 * trim);
        begin = end + 1;
    }
    // Every counted boundary quote is already in the mask.
    return outer_quotes == static_cast<unsigned>(
        __builtin_popcountll(static_cast<uint64_t>(quotes)) +
        __builtin_popcountll(static_cast<uint64_t>(quotes >> 64)));
}
#endif

template<size_t N>
bool fields(Text row, std::array<Text, N>& output) {
#ifdef __AVX2__
    if (row.size() >= 32 && row.size() <= 128) return fields_simd(row, output);
#endif
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

#ifdef __AVX2__
inline Time thirteen_digits(Text text) {
    int64_t first, last;
    std::memcpy(&first, text.data(), 8);
    std::memcpy(&last, text.data() + 5, 8);
    const auto bytes = _mm_sub_epi8(_mm_set_epi64x(last, first), _mm_set1_epi8('0'));
    const auto ordered = _mm_xor_si128(bytes, _mm_set1_epi8(-128));
    if (_mm_movemask_epi8(_mm_cmpgt_epi8(ordered, _mm_set1_epi8(9 - 128)))) return bad_time;
    const auto pairs = _mm_maddubs_epi16(bytes, _mm_set1_epi16(0x010a));
    const auto quads = _mm_madd_epi16(pairs, _mm_set1_epi32(0x00010064));
    const int upper = _mm_cvtsi128_si32(quads) * 10000 + _mm_extract_epi32(quads, 1);
    const int lower = _mm_extract_epi32(quads, 2) * 10000 + _mm_extract_epi32(quads, 3);
    return static_cast<Time>(upper / 1000) * 100000000 + lower;
}
#endif

inline Time digits(Text value, Time limit) {
#ifdef __AVX2__
    if (limit == std::numeric_limits<Time>::max() && value.size() == 13) return thirteen_digits(value);
#endif
    if (value.empty()) return bad_time;
    Time result = 0;
    if ((limit == std::numeric_limits<Time>::max() && value.size() <= 18) ||
        (limit == 10000000 && value.size() <= 7)) {
        for (unsigned char byte : value) {
            const unsigned digit = byte - static_cast<unsigned>('0');
            if (digit > 9) return bad_time;
            result = result * 10 + digit;
        }
        return result;
    }
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

struct DateCacheEntry {
    uint64_t prefix = 0;
    uint16_t suffix = 0;
    Time seconds = bad_time;
};

inline Time date_start(const char* p) {
    static thread_local std::array<DateCacheEntry, 128> cache;
    uint64_t prefix;
    uint16_t suffix;
    std::memcpy(&prefix, p, 8);
    std::memcpy(&suffix, p + 8, 2);
    const auto byte = [&](size_t i) { return static_cast<unsigned char>(p[i]); };
    DateCacheEntry& entry = cache[(byte(8) * 10 + byte(9) +
        byte(6) * 31 + byte(5) * 77 + byte(3) * 13) & (cache.size() - 1)];
    if (entry.prefix == prefix && entry.suffix == suffix) return entry.seconds;

    const int year = fixed_digits(p, 4), month = fixed_digits(p + 5, 2);
    const int day = fixed_digits(p + 8, 2);
    if (year < 0 || month < 1 || month > 12 || day < 1) return bad_time;
    static constexpr int month_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    const bool leap = month == 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    if (day > month_days[month - 1] + leap) return bad_time;
    entry = {prefix, suffix, civil_days(year, month, day) * (day_ms / 1000)};
    return entry.seconds;
}

template<bool Seconds = false>
inline Time timestamp(Text value, bool utc_only = false) {
    if constexpr (Seconds) utc_only = true;
    const size_t size = value.size();
    const bool f1 = size == 20 && value[19] == 'Z';
    const bool f2 = !utc_only && size == 25 && (value[19] == '+' || value[19] == '-');
    if (!f1 && !f2) {
        return utc_only ? bad_time : digits(value, std::numeric_limits<Time>::max());
    }
    if (value[4] != '-' || value[7] != '-' || value[10] != 'T' ||
        value[13] != ':' || value[16] != ':') return bad_time;
    const char* p = value.data();
    const Time start = date_start(p);
    const int hour = fixed_digits(p + 11, 2), minute = fixed_digits(p + 14, 2);
    const int second = fixed_digits(p + 17, 2);
    if (start == bad_time || hour < 0 || hour > 23 ||
        minute < 0 || minute > 59 || second < 0 || second > 59) return bad_time;
    int offset = 0;
    if (f2) {
        const int h = fixed_digits(p + 20, 2), m = fixed_digits(p + 23, 2);
        if (p[22] != ':' || h < 0 || h > 23 || m < 0 || m > 59) return bad_time;
        offset = (h * 60 + m) * (p[19] == '-' ? -1 : 1);
    }
    const Time seconds = start + (hour * 60 + minute - offset) * 60 + second;
    if constexpr (Seconds) return seconds;
    return seconds * 1000;
}

inline int32_t epoch_day(Time time) {
    return static_cast<int32_t>(time / day_ms - (time % day_ms < 0));
}
}
