#pragma once

#include <memory>
#include <atomic>
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
#include <thread>
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

constexpr uint64_t key_multiplier = UINT64_C(0xd6e8feb86659fd93);
constexpr uint64_t key_inverse_multiplier = UINT64_C(0xcfee444d8b59a89b);
static_assert(key_multiplier * key_inverse_multiplier == 1);

inline uint64_t hash_key(uint64_t key) {
    key = (key ^ (key >> 32)) * key_multiplier;
    return key ^ (key >> 32);
}

inline Key unmix_key(uint64_t key) {
    key = (key ^ (key >> 32)) * key_inverse_multiplier;
    return key ^ (key >> 32);
}

struct KeyHash {
    size_t operator()(Key key) const { return hash_key(key); }
};

template<class Value>
using KeyMap = std::unordered_map<Key, Value, KeyHash>;

struct HexWords {
    uint64_t low;
    uint64_t high;
};

template<char Prefix>
inline bool lower_hex_words(Text value, HexWords& words) {
#ifdef __AVX2__
    if (value.size() != 33 || value[0] != Prefix) return false;
    const auto raw = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(value.data() + 1));
    const auto digits = _mm256_sub_epi8(raw, _mm256_set1_epi8('0'));
    const auto letters = _mm256_sub_epi8(raw, _mm256_set1_epi8('a'));
    const auto numeric = _mm256_cmpeq_epi8(digits, _mm256_min_epu8(digits, _mm256_set1_epi8(9)));
    const auto lower = _mm256_cmpeq_epi8(letters, _mm256_min_epu8(letters, _mm256_set1_epi8(5)));
    if (_mm256_movemask_epi8(_mm256_or_si256(numeric, lower)) != -1) return false;
    const auto nibble = _mm256_add_epi8(_mm256_and_si256(raw, _mm256_set1_epi8(15)),
        _mm256_andnot_si256(numeric, _mm256_set1_epi8(9)));
    const auto pairs = _mm256_maddubs_epi16(nibble, _mm256_set1_epi16(0x0110));
    const auto packed = _mm_packus_epi16(_mm256_castsi256_si128(pairs), _mm256_extracti128_si256(pairs, 1));
    words.low = static_cast<uint64_t>(_mm_cvtsi128_si64(packed));
    words.high = static_cast<uint64_t>(_mm_extract_epi64(packed, 1));
    return true;
#else
    return false;
#endif
}

inline uint64_t hex_identity(const HexWords& words) {
    return hash_key(words.low ^ ((words.high << 19) | (words.high >> 45)));
}

class Keys {
    static constexpr Key wide = Key{1} << 63;
    static constexpr Key raw_suffix = wide | (Key{1} << 55);
    static constexpr Key user_prefix = Key{1} << 47;
    static constexpr Key byte_highs = UINT64_C(0x8080808080808080);
    static constexpr unsigned shard_bits = 8;
    static constexpr size_t shard_count = size_t{1} << shard_bits;
    static constexpr Key primary_tag = Key{1} << 54;
    static constexpr Key slot_limit = primary_tag >> shard_bits;
#ifndef SHOPPING_PRIMARY_BITS
#define SHOPPING_PRIMARY_BITS 20
#endif
    static constexpr size_t primary_size = size_t{1} << SHOPPING_PRIMARY_BITS;

    struct alignas(64) PrimaryKey {
        static constexpr size_t capacity = 40;
        static constexpr uint64_t empty = 0, building = 2;
        std::atomic<uint64_t> tag;
        uint8_t size;
        char bytes[capacity];

        PrimaryKey() : tag(empty) {}
        Text text() const { return Text(bytes, size); }
    };
    static_assert(sizeof(PrimaryKey) == 64);

    struct alignas(64) Shard {
        struct Slot {
            size_t hash = 0;
            size_t index = 0;
        };

        std::mutex mutex;
        std::vector<Text> strings;
        std::vector<Slot> slots;
        std::vector<std::unique_ptr<char[]>> blocks;
        size_t used = 0, capacity = 0;

        static size_t bucket(const std::vector<Slot>& table, size_t hash) {
            return hash & (table.size() - 1);
        }

        void grow() {
            std::vector<Slot> larger(slots.empty() ? 32 : slots.size() * 2);
            const size_t mask = larger.size() - 1;
            for (const Slot& old : slots) {
                if (!old.index) continue;
                size_t at = bucket(larger, old.hash);
                while (larger[at].index) at = (at + 1) & mask;
                larger[at] = old;
            }
            slots.swap(larger);
        }

        Text own(Text value) {
            if (value.size() > capacity - used) {
                const size_t next = std::max(value.size(), size_t{64 * 1024});
                std::unique_ptr<char[]> block(new char[next]);
                blocks.push_back(std::move(block));
                capacity = next;
                used = 0;
            }
            char* const location = blocks.back().get() + used;
            std::memcpy(location, value.data(), value.size());
            used += value.size();
            return Text(location, value.size());
        }

        size_t intern(Text value, size_t hash) {
            std::lock_guard<std::mutex> lock(mutex);
            if (slots.empty() || strings.size() >= slots.size() - slots.size() / 4) grow();
            const size_t mask = slots.size() - 1;
            size_t at = bucket(slots, hash);
            while (slots[at].index) {
                const Slot& item = slots[at];
                if (item.hash == hash && strings[item.index - 1] == value) return item.index - 1;
                at = (at + 1) & mask;
            }
            const size_t index = strings.size();
            if (index >= slot_limit) throw std::runtime_error("too many wide IDs");
            strings.push_back(own(value));
            slots[at] = {hash, index + 1};
            return index;
        }
    };

    std::array<Shard, shard_count> shards_;
    std::vector<PrimaryKey> primary_;
    bool frozen_ = false;

    Key try_primary(Text text, size_t hash) {
        if (text.size() > PrimaryKey::capacity) return 0;
        constexpr size_t mask = primary_size - 1;
        // A ready tag is odd. Full bytes still resolve any tag collision.
        const uint64_t ready = hash | uint64_t{1};
        size_t at = hash & mask;
        for (unsigned probe = 0; probe < 24; ++probe, at = (at + 1) & mask) {
            auto& slot = primary_[at];
            auto tag = slot.tag.load(std::memory_order_acquire);
            if (tag == PrimaryKey::empty &&
                slot.tag.compare_exchange_strong(tag, PrimaryKey::building,
                    std::memory_order_acq_rel, std::memory_order_acquire)) {
                slot.size = static_cast<uint8_t>(text.size());
                if (slot.size) std::memcpy(slot.bytes, text.data(), slot.size);
                slot.tag.store(ready, std::memory_order_release);
                return wide | primary_tag | at;
            }
            // Advancing before publication could give an equal string a second key.
            while (tag == PrimaryKey::building) {
                std::this_thread::yield();
                tag = slot.tag.load(std::memory_order_acquire);
            }
            if (tag == ready && slot.text() == text) return wide | primary_tag | at;
        }
        return 0;
    }

    static Text decode(Key key, char (&buffer)[9]) {
        size_t size = 0;
        while (key != 0) {
            buffer[size++] = static_cast<char>((key >> 56) & 127);
            key = (key << 7) & ~wide;
        }
        return Text(buffer, size);
    }

public:
    Keys() : primary_(primary_size) {}

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
        const size_t hash = std::hash<Text>{}(text);
        if (const Key primary = try_primary(text, hash)) return primary;
        const size_t shard = hash >> (64 - shard_bits);
        const size_t slot = shards_[shard].intern(text, hash);
        return wide | shard | (slot << shard_bits);
    }

    // All encoding workers have joined before consumers read the frozen store.
    void freeze() { frozen_ = true; }

    Text text(Key key, char (&buffer)[9]) {
        if ((key & raw_suffix) == raw_suffix) {
            buffer[0] = (key & user_prefix) ? 'u' : 'e';
            const Key suffix = key & ~(raw_suffix | user_prefix);
            std::memcpy(buffer + 1, &suffix, 8);
            return Text(buffer, 9);
        }
        if ((key & wide) == 0) return decode(key, buffer);
        if (key & primary_tag) {
            const auto& slot = primary_[key & (primary_size - 1)];
            (void)slot.tag.load(std::memory_order_acquire);
            return slot.text();
        }
        Shard& shard = shards_[key & (shard_count - 1)];
        const size_t slot = (key & ~wide) >> shard_bits;
        if (frozen_) return shard.strings[slot];
        std::lock_guard<std::mutex> lock(shard.mutex);
        return shard.strings[slot];
    }

    bool less(Key a, Key b) {
        if (((a | b) & wide) == 0) return a < b;
        if ((a & b & raw_suffix) == raw_suffix) {
            if ((a ^ b) & user_prefix) return (a & user_prefix) == 0;
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
            return __builtin_bswap64(a) < __builtin_bswap64(b);
#else
            return a < b;
#endif
        }
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
    if (bytes.empty()) return;
#if defined(__AVX2__)
    constexpr size_t block_size = 64;
    const auto lf = _mm256_set1_epi8('\n');
    const auto newline_mask = [&](const char* data) {
        const auto bytes = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data));
        return static_cast<uint64_t>(static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, lf))));
    };
    const char* start = bytes.data();
    const char* const end = start + bytes.size();
    const char* cursor = start;
    for (; end - cursor >= block_size; cursor += block_size) {
        uint64_t found = newline_mask(cursor);
        if constexpr (block_size == 64) found |= newline_mask(cursor + 32) << 32;
        while (found) {
            const char* newline = cursor + __builtin_ctzll(found);
            visit(Text(start, newline - start));
            start = newline + 1;
            found &= found - 1;
        }
    }
    for (; cursor != end; ++cursor) {
        if (*cursor == '\n') {
            visit(Text(start, cursor - start));
            start = cursor + 1;
        }
    }
    if (start != end) visit(Text(start, end - start));
#else
    while (!bytes.empty()) {
        const size_t newline = bytes.find('\n');
        if (newline == Text::npos) {
            visit(bytes);
            break;
        }
        visit(bytes.substr(0, newline));
        bytes.remove_prefix(newline + 1);
    }
#endif
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
    const auto low = static_cast<uint64_t>(commas);
    const auto high = static_cast<uint64_t>(commas >> 64);
    const unsigned low_count = __builtin_popcountll(low);
    if (low_count + __builtin_popcountll(high) != N - 1) return false;

#ifdef __BMI2__
    unsigned quoted = 0;
    if (quotes) {
        const DelimiterMask last = DelimiterMask{1} << (row.size() - 1);
        const DelimiterMask opening = (commas << 1) | 1;
        const DelimiterMask closing = (commas >> 1) | last;
        if (quotes & ~(opening ^ closing)) return false;
        const auto by_field = [&](DelimiterMask shifted) {
            return _pext_u64(static_cast<uint64_t>(shifted), low) |
                (_pext_u64(static_cast<uint64_t>(shifted >> 64), high) << low_count);
        };
        const unsigned left = (by_field(quotes >> 1) << 1) | (quotes & 1);
        const unsigned right = by_field(quotes << 1) | ((quotes & last) ? (1u << (N - 1)) : 0u);
        if (left != right) return false;
        quoted = left;
    }
#else
    unsigned outer_quotes = 0;
#endif

    size_t begin = 0;
    for (size_t i = 0; i < N; ++i) {
        const size_t end = i < N - 1 ? first_bit(commas) : row.size();
        commas &= commas - 1;
        const size_t length = end - begin;
#ifdef __BMI2__
        const size_t trim = (quoted >> i) & 1;
#else
        const size_t trim = length >= 2 && row[begin] == '"' && row[end - 1] == '"';
        outer_quotes += 2 * trim;
#endif
        output[i] = Text(row.data() + begin + trim, length - 2 * trim);
        begin = end + 1;
    }
#ifdef __BMI2__
    return true;
#else
    // Every counted boundary quote is already in the mask.
    return outer_quotes == static_cast<unsigned>(
        __builtin_popcountll(static_cast<uint64_t>(quotes)) +
        __builtin_popcountll(static_cast<uint64_t>(quotes >> 64)));
#endif
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

struct UtcDate {
    Time seconds = bad_time;
    int32_t day = 0;
};

struct DateCacheEntry {
    uint64_t prefix = 0;
    uint16_t suffix = 0;
    uint16_t reserved = 0;
    int32_t day = 0;
    Time seconds = bad_time;
};

inline UtcDate utc_date_impl(const char* p
#ifdef __AVX2__
                            , __m128i input
#endif
) {
    static thread_local std::array<DateCacheEntry, 128> cache;
    // Adjacent months map to different halves. Each half has a slot for every day.
    const size_t slot = (static_cast<unsigned char>(p[9]) & 15)
        | ((static_cast<unsigned char>(p[8]) & 3) << 4)
        | ((static_cast<unsigned char>(p[6]) & 1) << 6);
    DateCacheEntry& entry = cache[slot];
    uint64_t prefix;
    uint16_t suffix;
#ifdef __AVX2__
    const auto cached = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&entry));
    const auto date_mask = _mm_set_epi64x(UINT64_C(0xffff), -1);
    const auto diff = _mm_xor_si128(input, cached);
    if (_mm_testz_si128(diff, date_mask)) return {entry.seconds, entry.day};
    prefix = static_cast<uint64_t>(_mm_cvtsi128_si64(input));
    suffix = static_cast<uint16_t>(_mm_extract_epi16(input, 4));
#else
    std::memcpy(&prefix, p, 8);
    std::memcpy(&suffix, p + 8, 2);
    if (entry.prefix == prefix && entry.suffix == suffix) return {entry.seconds, entry.day};
#endif
    if (p[4] != '-' || p[7] != '-') return {};

    const int year = fixed_digits(p, 4), month = fixed_digits(p + 5, 2);
    const int day = fixed_digits(p + 8, 2);
    if (year < 0 || month < 1 || month > 12 || day < 1) return {};
    static constexpr int month_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    const bool leap = month == 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    if (day > month_days[month - 1] + leap) return {};
    const auto epoch = static_cast<int32_t>(civil_days(year, month, day));
    entry = {prefix, suffix, 0, epoch, epoch * (day_ms / 1000)};
    return {entry.seconds, entry.day};
}

inline UtcDate utc_date(const char* p) {
#ifdef __AVX2__
    // F1 and F2 both provide at least 20 bytes. Only the first ten form the date.
    return utc_date_impl(p, _mm_loadu_si128(reinterpret_cast<const __m128i*>(p)));
#else
    return utc_date_impl(p);
#endif
}

constexpr unsigned ingest_clock_bits = 24;
constexpr int32_t ingest_day_bias = 1 << 20;

struct IngestClock {
    uint64_t order = 0;
    Time midnight_ms = 0;

    bool timely(Time event) const {
        if (midnight_ms <= event) return true;
        const Time deadline = event + day_ms;
        if (midnight_ms > deadline) return false;
        const auto pair = [&](unsigned shift) {
            return 10 * ((order >> (shift + 4)) & 15) + ((order >> shift) & 15);
        };
        const auto second = 3600 * pair(16) + 60 * pair(8) + pair(0);
        return midnight_ms + static_cast<Time>(second) * 1000 <= deadline;
    }
};

inline IngestClock ingest_clock(Text value) {
    if (value.size() != 20 || value[10] != 'T' || value[19] != 'Z') return {};
    const char* p = value.data();
    uint32_t clock;
#ifdef __BMI2__
    uint64_t word;
    std::memcpy(&word, p + 11, sizeof(word));
    word = __builtin_bswap64(word);
    constexpr uint64_t digits = UINT64_C(0x0f0f000f0f000f0f);
    if ((word & ~digits) != UINT64_C(0x30303a30303a3030) ||
        (((word & digits) + UINT64_C(0x0606000606000606)) & UINT64_C(0x1010001010001010))) return {};
    clock = static_cast<uint32_t>(_pext_u64(word, digits));
    if ((clock >> 16) > 0x23 || ((clock >> 8) & 255) > 0x59 || (clock & 255) > 0x59) return {};
#else
    if (p[13] != ':' || p[16] != ':') return {};
    const int hours = fixed_digits(p + 11, 2);
    const int minutes = fixed_digits(p + 14, 2);
    const int seconds = fixed_digits(p + 17, 2);
    if (hours < 0 || hours > 23 || minutes < 0 || minutes > 59 || seconds < 0 || seconds > 59) return {};
    const auto bcd = [](unsigned n) { return ((n / 10) << 4) | (n % 10); };
    clock = (bcd(hours) << 16) | (bcd(minutes) << 8) | bcd(seconds);
#endif
    const UtcDate date = utc_date(p);
    if (date.seconds == bad_time) return {};
    return {(static_cast<uint64_t>(date.day + ingest_day_bias) << ingest_clock_bits) | clock,
            date.seconds * 1000};
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
    if (value[10] != 'T' || value[13] != ':' || value[16] != ':') return bad_time;
    const char* p = value.data();
    const Time start = utc_date(p).seconds;
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

struct EventTimes {
    IngestClock ingest;
    Time event;
};

#if defined(__AVX2__) && defined(__BMI2__)
inline EventTimes same_date_times(Text event, Text ingest) {
    if (event.size() != 20 || ingest.size() != 20 ||
        event[10] != 'T' || ingest[10] != 'T' || event[19] != 'Z' || ingest[19] != 'Z') return {{}, bad_time};

    const auto event_date = _mm_loadu_si128(reinterpret_cast<const __m128i*>(event.data()));
    const auto ingest_date = _mm_loadu_si128(reinterpret_cast<const __m128i*>(ingest.data()));
    const auto date_mask = _mm_set_epi64x(UINT64_C(0xffff), -1);
    if (!_mm_testz_si128(_mm_xor_si128(event_date, ingest_date), date_mask)) return {{}, bad_time};

    uint64_t event_word, ingest_word;
    std::memcpy(&event_word, event.data() + 11, 8);
    std::memcpy(&ingest_word, ingest.data() + 11, 8);
    const auto clocks = _mm_set_epi64x(ingest_word, event_word);
    constexpr auto digit_mask = UINT64_C(0x0f0f000f0f000f0f);
    const auto digit_lanes = _mm_set1_epi64x(digit_mask);
    const auto required = _mm_set1_epi64x(UINT64_C(0x30303a30303a3030));
    const auto syntax = _mm_xor_si128(_mm_andnot_si128(digit_lanes, clocks), required);
    const auto digits = _mm_and_si128(clocks, digit_lanes);
    const auto oversized = _mm_and_si128(_mm_add_epi8(digits, _mm_set1_epi8(6)), _mm_set1_epi8(16));
    const auto failures = _mm_or_si128(syntax, oversized);
    if (!_mm_testz_si128(failures, failures)) return {{}, bad_time};

    const auto compact = _mm_shuffle_epi8(digits, _mm_setr_epi8(0, 1, 3, 4, 6, 7, -1, -1,
                                                                 8, 9, 11, 12, 14, 15, -1, -1));
    const auto pairs = _mm_maddubs_epi16(compact, _mm_set1_epi16(0x010a));
    if (_mm_movemask_epi8(_mm_cmpgt_epi16(pairs, _mm_setr_epi16(23, 59, 59, 0, 23, 59, 59, 0)))) {
        return {{}, bad_time};
    }
    const UtcDate date = utc_date_impl(event.data(), event_date);
    if (date.seconds == bad_time) return {{}, bad_time};
    const auto weighted = _mm_madd_epi16(pairs, _mm_setr_epi16(3600, 60, 1, 0, 0, 0, 0, 0));
    const int32_t seconds = _mm_cvtsi128_si32(weighted) + _mm_extract_epi32(weighted, 1);
    const auto order = (static_cast<uint64_t>(date.day + ingest_day_bias) << ingest_clock_bits) |
        _pext_u64(__builtin_bswap64(ingest_word), digit_mask);
    return {{order, date.seconds * 1000}, (date.seconds + seconds) * 1000};
}
#endif

inline EventTimes event_times(Text event, Text ingest) {
#if defined(__AVX2__) && defined(__BMI2__)
    const EventTimes pair = same_date_times(event, ingest);
    if (pair.ingest.order) return pair;
#endif
    const IngestClock clock = ingest_clock(ingest);
    return {clock, clock.order ? timestamp(event) : bad_time};
}

inline int32_t epoch_day(Time time) {
    return static_cast<int32_t>(time / day_ms - (time % day_ms < 0));
}
}
