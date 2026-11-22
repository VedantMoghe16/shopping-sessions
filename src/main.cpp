#include "input.hpp"
#include "output.hpp"

#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <exception>
#include <future>
#include <iostream>
#include <memory>
#include <numeric>
#include <thread>
#include <utility>
#include <new>
#include <type_traits>
#include <nlohmann/json.hpp>

extern "C" int __llvm_profile_write_file() noexcept __attribute__((weak));

namespace shopping {
constexpr size_t partitions = 256;
constexpr size_t chunk_bytes = 4 * 1024 * 1024;
constexpr Time session_gap = 1800000;
#ifndef SHOPPING_BUCKET_THRESHOLD
#define SHOPPING_BUCKET_THRESHOLD 16
#endif
constexpr size_t bucket_threshold = SHOPPING_BUCKET_THRESHOLD;

size_t partition(Key key) { return hash_key(key) & (partitions - 1); }

template<class Work>
void parallel(size_t workers, size_t tasks, Work work) {
    std::atomic<size_t> next{0};
    std::mutex error_mutex;
    std::exception_ptr error;
    auto run = [&](size_t worker) {
        try {
            for (size_t task; (task = next.fetch_add(1, std::memory_order_relaxed)) < tasks;) {
                work(worker, task);
            }
        } catch (...) {
            next.store(tasks, std::memory_order_relaxed);
            std::lock_guard<std::mutex> lock(error_mutex);
            if (!error) error = std::current_exception();
        }
    };
    std::vector<std::thread> threads;
    for (size_t i = 1; i < workers; ++i) threads.emplace_back(run, i);
    run(0);
    for (auto& thread : threads) thread.join();
    if (error) std::rethrow_exception(error);
}

struct Inputs {
    std::vector<std::unique_ptr<MappedFile>> files;
    std::vector<std::unique_ptr<fs::path>> paths;

    Text map(const fs::path& path) {
        files.push_back(std::make_unique<MappedFile>(path));
        return files.back()->bytes();
    }

    template<class Visit>
    void folder(const fs::path& path, Visit visit) {
        for (const auto& file : fs::directory_iterator(path)) {
            if (file.path().extension() == ".csv" && file.is_regular_file()) {
                const Text bytes = map(file.path());
                paths.push_back(std::make_unique<fs::path>(file.path()));
                visit(bytes, *paths.back());
            }
        }
    }

    template<class Visit>
    void dimension(const fs::path& path, Visit visit) {
        const Text bytes = map(path);
        lines(bytes.substr(next_line(bytes, 0)), visit);
    }
};

class Links {
    struct Node {
        Key minimum;
        uint32_t parent, size;
    };
    struct Entry {
        Key mixed = 0, value = 0;
    };
    std::vector<Entry> ids_ = std::vector<Entry>(1024);
    std::vector<Node> nodes_;
    std::vector<uint64_t> changed_;

    static Entry& find(std::vector<Entry>& table, Key mixed) {
        const size_t mask = table.size() - 1;
        size_t slot = mixed & mask;
        while (table[slot].mixed && table[slot].mixed != mixed) slot = (slot + 1) & mask;
        return table[slot];
    }

    uint32_t add(Key key) {
        const Key mixed = hash_key(key);
        Entry* entry = &find(ids_, mixed);
        if (entry->mixed == 0) {
            if ((nodes_.size() + 1) * 4 > ids_.size() * 3) {
                std::vector<Entry> larger(ids_.size() * 2);
                for (const Entry& old : ids_) {
                    if (old.mixed) find(larger, old.mixed) = old;
                }
                ids_.swap(larger);
                entry = &find(ids_, mixed);
            }
            if (nodes_.size() == UINT32_MAX) throw std::runtime_error("too many linked IDs");
            const auto index = static_cast<uint32_t>(nodes_.size());
            *entry = {mixed, index};
            nodes_.push_back({key, index, 1});
        }
        return static_cast<uint32_t>(entry->value);
    }

    uint32_t root(uint32_t index) {
        while (index != nodes_[index].parent) {
            nodes_[index].parent = nodes_[nodes_[index].parent].parent;
            index = nodes_[index].parent;
        }
        return index;
    }

public:
    void join(Key a, Key b, Keys& keys) {
        uint32_t x = root(add(a));
        uint32_t y = root(add(b));
        if (x == y) return;
        if (nodes_[x].size < nodes_[y].size) std::swap(x, y);
        nodes_[y].parent = x;
        nodes_[x].size += nodes_[y].size;
        if (keys.less(nodes_[y].minimum, nodes_[x].minimum)) nodes_[x].minimum = nodes_[y].minimum;
    }

    void finish() {
        size_t aliases = 0;
        for (Entry& entry : ids_) {
            if (entry.mixed == 0) continue;
            const Key canonical = nodes_[root(static_cast<uint32_t>(entry.value))].minimum;
            entry.value = hash_key(canonical);
            aliases += entry.value != entry.mixed;
        }
        std::vector<Node>().swap(nodes_);
        // A clear bit proves the user is already canonical. A set bit still needs an exact lookup.
        size_t bits = 64;
        while (bits < 16 * aliases) bits *= 2;
        changed_.resize(bits / 64);
        for (const Entry& entry : ids_) {
            if (entry.mixed != entry.value) {
                const size_t index = entry.mixed & (bits - 1);
                changed_[index >> 6] |= uint64_t{1} << (index & 63);
            }
        }
    }

    Key changed_person(Key mixed) const {
        // The reversible mixer preserves full key equality.
        const size_t mask = ids_.size() - 1;
        size_t slot = mixed & mask;
        while (ids_[slot].mixed && ids_[slot].mixed != mixed) slot = (slot + 1) & mask;
        return ids_[slot].mixed == mixed ? ids_[slot].value : mixed;
    }

    Key mixed_person(Key user) const {
        const Key mixed = hash_key(user);
        const size_t index = mixed & (changed_.size() * 64 - 1);
        if ((changed_[index >> 6] & (uint64_t{1} << (index & 63))) == 0) return mixed;
        return changed_person(mixed);
    }

    template<class Row, size_t N>
    void resolve_batch(std::array<Row, N>& rows, size_t count) const {
        static_assert(N <= 64);
        const size_t bits = changed_.size() * 64 - 1;
        const size_t mask = ids_.size() - 1;
        for (size_t i = 0; i < count; ++i) {
            rows[i].person = hash_key(rows[i].person);
            __builtin_prefetch(&changed_[(rows[i].person & bits) >> 6], 0, 1);
        }
        uint64_t pending = 0;
        for (size_t i = 0; i < count; ++i) {
            const Key mixed = rows[i].person;
            const size_t bit = mixed & bits;
            if ((changed_[bit >> 6] & (uint64_t{1} << (bit & 63))) != 0) {
                pending |= uint64_t{1} << i;
                __builtin_prefetch(&ids_[mixed & mask], 0, 1);
            }
        }
        while (pending) {
            const size_t i = __builtin_ctzll(pending);
            rows[i].person = changed_person(rows[i].person);
            pending &= pending - 1;
        }
    }
};

constexpr uint32_t outside_campaign_index = 10000;

uint32_t campaign_number(Text name) {
    if (name.size() < 3 || name.size() > 6 || name[0] != 'c' || name[1] != '_' ||
        (name.size() > 3 && name[2] == '0')) return outside_campaign_index;
    unsigned index = 0;
    for (size_t i = 2; i < name.size(); ++i) {
        const unsigned digit = static_cast<unsigned char>(name[i]) - static_cast<unsigned>('0');
        if (digit > 9) return outside_campaign_index;
        index = index * 10 + digit;
    }
    return index;
}

struct Catalog {
    KeyMap<uint32_t> products, campaigns, category_ids;
    std::vector<uint32_t> numbered_campaigns;
    std::vector<Key> categories;
    std::vector<std::pair<Key, Key>> names;

    uint32_t campaign(Text name, Keys& keys) const {
        // Only canonical decimal suffixes use the direct index.
        const uint32_t index = campaign_number(name);
        if (index < outside_campaign_index) {
            return index < numbered_campaigns.size() ? numbered_campaigns[index] : 0;
        }
        const auto found = campaigns.find(keys.encode(name));
        return found == campaigns.end() ? 0 : found->second;
    }
};

Catalog load_catalog(Inputs& inputs, const fs::path& data, Keys& keys) {
    Catalog catalog;
    const Key unknown = keys.encode("unknown");
    const Key organic = keys.encode("organic");
    catalog.categories.push_back(unknown);
    catalog.category_ids.emplace(unknown, 0);
    catalog.names.emplace_back(organic, organic);
    std::array<Text, 2> columns;
    inputs.dimension(data / "products.csv", [&](Text row) {
        if (!fields(row, columns)) throw std::runtime_error("invalid product dimension");
        const Key name = keys.encode(columns[1]);
        const auto category = catalog.category_ids.try_emplace(name, catalog.categories.size());
        if (category.second) catalog.categories.push_back(name);
        catalog.products.emplace(keys.encode(columns[0]), category.first->second);
    });
    inputs.dimension(data / "campaigns.csv", [&](Text row) {
        if (!fields(row, columns)) throw std::runtime_error("invalid campaign dimension");
        const Key id = keys.encode(columns[0]);
        const uint32_t slot = campaign_number(columns[0]);
        const auto campaign = static_cast<uint32_t>(catalog.names.size());
        if (slot < outside_campaign_index) {
            if (catalog.numbered_campaigns.size() <= slot) catalog.numbered_campaigns.resize(slot + 1);
            if (catalog.numbered_campaigns[slot] == 0) catalog.numbered_campaigns[slot] = campaign;
        } else {
            catalog.campaigns.emplace(id, campaign);
        }
        catalog.names.emplace_back(id, keys.encode(columns[1]));
    });
    return catalog;
}

struct TimedEvent {
    Time time;
    uint32_t cents, category;
};

struct WideEvents {
    struct Event {
        Key person;
        TimedEvent data;
    };

    Event make(Key person, Time time, uint32_t cents, uint32_t category) const {
        return {person, {time, cents, category}};
    }

    TimedEvent decode(const Event& event) const { return event.data; }

    struct Candidate {
        Key id;
        uint64_t ingest;
        Event event;
    };

    Candidate candidate(Key id, uint64_t ingest, Event event, std::vector<Candidate>&, size_t) const {
        return {id, ingest, event};
    }

    template<class Workers>
    uint64_t ingest(const Candidate& row, const Workers&) const { return row.ingest; }

    template<class Workers>
    Event event(const Candidate& row, const Workers&) const { return row.event; }
};

struct CompactEvents {
    static constexpr uint32_t price_bits = 24;
    static constexpr uint32_t price_mask = (uint32_t{1} << price_bits) - 1;

    struct Event {
        Key person;
        uint32_t offset, purchase;
    };

    Time start;
    uint64_t ingest_base;

    Event make(Key person, Time time, uint32_t cents, uint32_t category) const {
        return {person, static_cast<uint32_t>(time - start), (category << price_bits) | cents};
    }

    TimedEvent decode(const Event& event) const {
        return {start + event.offset, event.purchase & price_mask, event.purchase >> price_bits};
    }

    struct Candidate {
        Key id, person;
        uint64_t packed;
    };

    static constexpr uint64_t indirect = uint64_t{1} << 63;
    static constexpr uint64_t extra_index_mask = (uint64_t{1} << 48) - 1;
    static constexpr uint64_t time_mask = (uint64_t{1} << 31) - 1;

    Candidate candidate(Key id, uint64_t ingest, Event event, std::vector<WideEvents::Candidate>& extra, size_t worker) const {
        const uint64_t clock = ingest - ingest_base;
        // The relative day and clock use 32 bits. Side records keep absolute keys.
        if (event.purchase == 0 && clock <= UINT32_MAX) {
            return {id, event.person, (clock << 31) | event.offset};
        }
        if (extra.size() > extra_index_mask) throw std::runtime_error("too many indirect events");
        const uint64_t pointer = indirect | (static_cast<uint64_t>(worker) << 48) | extra.size();
        extra.push_back({id, ingest, {event.person, decode(event)}});
        return {id, event.person, pointer};
    }

    template<class Workers>
    const WideEvents::Candidate& extra(const Candidate& row, const Workers& workers) const {
        const size_t worker = (row.packed >> 48) & 0x7fff;
        return workers[worker].extra[row.packed & extra_index_mask];
    }

    template<class Workers>
    uint64_t ingest(const Candidate& row, const Workers& workers) const {
        if (row.packed & indirect) return extra(row, workers).ingest;
        return ingest_base + (row.packed >> 31);
    }

    template<class Workers>
    Event event(const Candidate& row, const Workers& workers) const {
        if (row.person == 0) return {};
        if (row.packed & indirect) {
            const TimedEvent& event = extra(row, workers).event.data;
            return make(row.person, event.time, event.cents, event.category);
        }
        return {row.person, static_cast<uint32_t>(row.packed & time_mask), 0};
    }
};

struct Click {
    Key person;
    Time time;
    Key id;
    uint32_t campaign;
    uint32_t opaque_size;
};

constexpr size_t arena_slab_bytes = 2 * 1024 * 1024;

class RecordArena {
    std::vector<char*> slabs_;
    size_t used_ = arena_slab_bytes;

public:
    RecordArena() = default;
    RecordArena(const RecordArena&) = delete;
    RecordArena& operator=(const RecordArena&) = delete;
    ~RecordArena() { release(); }

    void release() {
        for (char* slab : slabs_) munmap(slab, arena_slab_bytes);
        slabs_.clear();
        used_ = arena_slab_bytes;
    }

    void* allocate(size_t bytes) {
        if (used_ + bytes > arena_slab_bytes) {
            if (slabs_.size() == slabs_.capacity()) {
                slabs_.reserve(std::max(size_t{4}, 2 * slabs_.capacity()));
            }
            void* memory = mmap(nullptr, 2 * arena_slab_bytes, PROT_READ | PROT_WRITE,
                                MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            if (memory == MAP_FAILED) throw std::bad_alloc();
            const auto address = reinterpret_cast<uintptr_t>(memory);
            const auto aligned = (address + arena_slab_bytes - 1) & ~(arena_slab_bytes - 1);
            auto* slab = reinterpret_cast<char*>(aligned);
            const size_t before = aligned - address;
            if (before != 0) munmap(memory, before);
            munmap(slab + arena_slab_bytes, arena_slab_bytes - before);
            // The advice is optional. The arena works with ordinary pages too.
            madvise(slab, arena_slab_bytes, MADV_HUGEPAGE);
            slabs_.push_back(slab);
            used_ = 0;
        }
        char* slice = slabs_.back() + used_;
        used_ += bytes;
        return slice;
    }
};

template<class Record, size_t page_rows = 2048>
class RecordBuffers {
    static_assert(sizeof(Record) * page_rows <= arena_slab_bytes);
    static_assert(std::is_trivially_destructible_v<Record>);
    std::vector<Record*> pages_;
    RecordArena* arena_ = nullptr;
    size_t size_ = 0;

public:
    void set_arena(RecordArena& arena) { arena_ = &arena; }
    size_t size() const { return size_; }

    void push_back(const Record& row) {
        const size_t offset = size_ % page_rows;
        if (offset == 0) {
            void* memory = arena_->allocate(sizeof(Record) * page_rows);
            pages_.push_back(::new (memory) Record[page_rows]);
        }
        pages_.back()[offset] = row;
        ++size_;
    }

    template<class Visit>
    void each(Visit visit) {
        size_t remaining = size_;
        for (const auto& page : pages_) {
            const size_t count = std::min(page_rows, remaining);
            for (size_t i = 0; i < count; ++i) visit(page[i]);
            remaining -= count;
        }
    }

    void clear() {
        pages_.clear();
        size_ = 0;
    }
};

template<class Candidate>
struct OpaqueCandidate : Candidate {
    const char* identity;
    size_t size;
};

template<class Layout>
struct WorkerInput {
    RecordArena candidate_storage, event_storage;
    std::array<RecordBuffers<typename Layout::Candidate>, partitions> candidates;
    std::array<RecordBuffers<OpaqueCandidate<typename Layout::Candidate>, 512>, partitions> opaque_candidates;
    std::vector<WideEvents::Candidate> extra;
    std::array<RecordBuffers<typename Layout::Event, 512>, partitions> events;
    std::array<std::vector<Click>, partitions> clicks;

    WorkerInput() {
        for (auto& buffer : candidates) buffer.set_arena(candidate_storage);
        for (auto& buffer : opaque_candidates) buffer.set_arena(candidate_storage);
        for (auto& buffer : events) buffer.set_arena(event_storage);
    }
};

struct Chunk {
    size_t offset, size;
    const fs::path* path;
    const char* mapped;
    bool events;
};

void load_chunks(Inputs& inputs, const fs::path& path, bool events, std::vector<Chunk>& chunks) {
    inputs.folder(path, [&](Text bytes, const fs::path& source) {
        size_t begin = next_line(bytes, 0);
        while (begin < bytes.size()) {
            size_t end = std::min(begin + chunk_bytes, bytes.size());
            if (end != bytes.size() && bytes[end - 1] != '\n') end = next_line(bytes, end);
            chunks.push_back({begin, end - begin, &source, bytes.data() + begin, events});
            begin = end;
        }
    });
}

class ChunkReader {
    std::unique_ptr<char[]> buffer_;
    size_t capacity_ = 0;

public:
    Text read(const Chunk& chunk) {
        // A single long row can extend a chunk past the usual buffer size.
        if (chunk.size > 2 * chunk_bytes) return Text(chunk.mapped, chunk.size);
        if (capacity_ < chunk.size) {
            buffer_.reset(new char[chunk.size]);
            capacity_ = chunk.size;
        }
        const int fd = open(chunk.path->c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("open " + chunk.path->string());
        size_t offset = 0;
        while (offset < chunk.size) {
            const auto got = pread(fd, buffer_.get() + offset, chunk.size - offset, chunk.offset + offset);
            if (got <= 0) {
                if (got < 0 && errno == EINTR) continue;
                close(fd);
                throw std::runtime_error("read " + chunk.path->string());
            }
            offset += static_cast<size_t>(got);
        }
        close(fd);
        return Text(buffer_.get(), chunk.size);
    }
};

template<class Layout>
void event_chunk(Text bytes, const char* mapped, Keys& keys, const Catalog& catalog,
                 WorkerInput<Layout>& worker, size_t worker_id, Time start, Time end,
                 const Layout& layout) {
    std::array<Text, 7> c;
    lines(bytes, [&](Text row) {
        if (!fields(row, c) || c[0].empty()) return;
        const auto [ingest, time] = event_times(c[2], c[3]);
        if (ingest.order == 0) throw std::runtime_error("invalid guaranteed ingest_ts");
        typename Layout::Event event{};
        const bool purchase = c[4] == "purchase";
        if (!c[1].empty() && !c[5].empty() && (purchase || c[4] == "view" || c[4] == "cart")
            && time >= start && time < end && ingest.timely(time)) {
            const Time price = purchase ? digits(c[6], 10000000) : 0;
            if (!purchase || price > 0) {
                uint32_t category = 0;
                if (purchase) {
                    const auto found = catalog.products.find(keys.encode(c[5]));
                    if (found != catalog.products.end()) category = found->second;
                }
                event = layout.make(keys.encode(c[1]), time, static_cast<uint32_t>(price), category);
            }
        }
        if (c[0].size() <= 9) {
            const Key id = hash_key(keys.encode(c[0]));
            worker.candidates[id & (partitions - 1)].push_back(
                layout.candidate(id, ingest.order, event, worker.extra, worker_id));
        } else {
            const Key fingerprint = std::hash<Text>{}(c[0]);
            const char* identity = mapped + (c[0].data() - bytes.data());
            worker.opaque_candidates[fingerprint & (partitions - 1)].push_back(
                {layout.candidate(fingerprint, ingest.order, event, worker.extra, worker_id),
                 identity, c[0].size()});
        }
    });
}

template<class Layout>
void click_chunk(Text bytes, const char* mapped, Keys& keys, const Catalog& catalog, const Links& links,
                 WorkerInput<Layout>& worker, Time start, Time end) {
    std::array<Text, 4> c;
    lines(bytes, [&](Text row) {
        if (!fields(row, c) || c[0].empty() || c[1].empty() || c[2].empty()) return;
        const Time time = timestamp(c[3]);
        if (time < start - week_ms || time >= end) return;
        const uint32_t campaign = catalog.campaign(c[2], keys);
        if (campaign == 0) return;
        const Key person = links.mixed_person(keys.encode(c[1]));
        const auto size = c[0].size();
        if (size > 9 && size <= UINT32_MAX) {
            const auto* address = mapped + (c[0].data() - bytes.data());
            worker.clicks[person & (partitions - 1)].push_back(
                {person, time, reinterpret_cast<uintptr_t>(address), campaign, static_cast<uint32_t>(size)});
        } else {
            worker.clicks[person & (partitions - 1)].push_back({person, time, keys.encode(c[0]), campaign, 0});
        }
    });
}

template<bool Opaque, class Layout>
void deduplicate(size_t part, std::vector<WorkerInput<Layout>>& workers, WorkerInput<Layout>& writer,
                 const Links& links, const Layout& layout) {
    using Candidate = typename Layout::Candidate;
    using Record = std::conditional_t<Opaque, OpaqueCandidate<Candidate>, Candidate>;
    using Event = typename Layout::Event;
    const auto buffers = [part](auto& worker) -> auto& {
        if constexpr (Opaque) return worker.opaque_candidates[part];
        else return worker.candidates[part];
    };
    size_t count = 0;
    for (const auto& worker : workers) count += buffers(worker).size();
    if (!count) return;
    size_t buckets = 64;
    while (buckets < 16 * count) buckets *= 2;
    std::vector<uint64_t> seen(buckets / 64), collided(buckets / 64);
    size_t repeats = 0;
    for (auto& worker : workers) {
        buffers(worker).each([&](const Record& row) {
            const size_t bucket = (row.id >> 8) & (buckets - 1);
            const uint64_t bit = uint64_t{1} << (bucket & 63);
            const size_t word = bucket >> 6;
            const uint64_t hit = seen[word] & bit;
            repeats += hit != 0;
            collided[word] |= hit;
            seen[word] |= bit;
        });
    }
    size_t slots = 1;
    // Every collided bucket contains at most twice its number of repeat visits.
    while (slots < repeats * 3 + 1) slots *= 2;
    std::vector<const Record*> winners(slots, nullptr);
    std::array<Event, 32> pending;
    size_t pending_count = 0;
    auto flush = [&] {
        links.resolve_batch(pending, pending_count);
        for (size_t i = 0; i < pending_count; ++i) {
            const Event& event = pending[i];
            writer.events[event.person & (partitions - 1)].push_back(event);
        }
        pending_count = 0;
    };
    auto emit = [&](Event event) {
        if (event.person == 0) return;
        pending[pending_count++] = event;
        if (pending_count == pending.size()) flush();
    };
    for (auto& worker : workers) {
        buffers(worker).each([&](const Record& row) {
            const uint64_t hash = row.id >> 8;
            const size_t bucket = hash & (buckets - 1);
            if ((collided[bucket >> 6] & (uint64_t{1} << (bucket & 63))) == 0) {
                emit(layout.event(row, workers));
            } else {
                size_t i = hash & (slots - 1);
                while (const Record* winner = winners[i]) {
                    if (winner->id == row.id) {
                        if constexpr (!Opaque) break;
                        else if (Text(winner->identity, winner->size) == Text(row.identity, row.size)) break;
                    }
                    i = (i + 1) & (slots - 1);
                }
                if (!winners[i] || layout.ingest(row, workers) < layout.ingest(*winners[i], workers)) winners[i] = &row;
            }
        });
    }
    for (const Record* winner : winners) {
        if (winner) emit(layout.event(*winner, workers));
    }
    if (pending_count) flush();
    for (auto& worker : workers) buffers(worker).clear();
}

struct PersonGroup {
    Key person;
    size_t events = 0, clicks = 0, event_end = 0, click_end = 0;
};

class PersonGroups {
    std::vector<uint32_t> slots_ = std::vector<uint32_t>(256);

    size_t slot(Key person) const {
        const size_t mask = slots_.size() - 1;
        size_t index = (person >> 8) & mask;
        while (slots_[index] != 0 && groups[slots_[index] - 1].person != person) {
            index = (index + 1) & mask;
        }
        return index;
    }

public:
    std::vector<PersonGroup> groups;

    uint32_t add(Key person) {
        size_t index = slot(person);
        if (!slots_[index]) {
            if (groups.size() == UINT32_MAX) throw std::runtime_error("too many people in partition");
            if ((groups.size() + 1) * 4 > slots_.size() * 3) {
                slots_.assign(slots_.size() * 2, 0);
                for (size_t i = 0; i < groups.size(); ++i) {
                    slots_[slot(groups[i].person)] = i + 1;
                }
                index = slot(person);
            }
            groups.push_back({person});
            slots_[index] = groups.size();
        }
        return slots_[index] - 1;
    }

    uint32_t lookup(Key person) const { return slots_[slot(person)]; }
};

struct TimedClick {
    Time time;
    Key id;
    uint32_t campaign;
    uint32_t opaque_size;
};

bool click_id_less(const TimedClick& a, const TimedClick& b, Keys& keys) {
    if ((a.opaque_size | b.opaque_size) == 0) return keys.less(a.id, b.id);
    char a_buffer[9], b_buffer[9];
    const auto text = [&](const TimedClick& click, char (&buffer)[9]) {
        return click.opaque_size ? Text(reinterpret_cast<const char*>(click.id), click.opaque_size)
                                 : keys.text(click.id, buffer);
    };
    return text(a, a_buffer).compare(text(b, b_buffer)) < 0;
}

struct PersonPartition {
    std::vector<TimedEvent> events;
    std::vector<TimedClick> clicks;
    std::vector<PersonGroup> groups;
};

template<class Layout>
PersonPartition group_by_person(std::vector<WorkerInput<Layout>>& workers, size_t part,
                                size_t bucket_count, const Layout& layout) {
    using Event = typename Layout::Event;
    PersonGroups index;
    size_t event_count = 0, click_count = 0;
    for (auto& worker : workers) {
        worker.events[part].each([&](Event& event) {
            event.person = index.add(event.person);
            ++index.groups[event.person].events;
        });
        event_count += worker.events[part].size();
    }
    for (auto& worker : workers) {
        for (Click& click : worker.clicks[part]) {
            click.person = index.lookup(click.person);
            if (click.person) {
                ++index.groups[click.person - 1].clicks;
                ++click_count;
            }
        }
    }
    size_t event_offset = 0, click_offset = 0;
    for (PersonGroup& group : index.groups) {
        group.event_end = event_offset += group.events;
        group.click_end = click_offset += group.clicks;
    }
    PersonPartition result;
    result.events.resize(event_count);
    result.clicks.resize(click_count);
    for (auto& worker : workers) {
        worker.events[part].each([&](const Event& event) {
            PersonGroup& group = index.groups[event.person];
            result.events[--group.event_end] = layout.decode(event);
        });
        worker.events[part].clear();
        for (const Click& click : worker.clicks[part]) {
            if (click.person) {
                PersonGroup& group = index.groups[click.person - 1];
                result.clicks[--group.click_end] = {click.time, click.id, click.campaign, click.opaque_size};
            }
        }
        std::vector<Click>().swap(worker.clicks[part]);
    }
    const auto earlier = [](const auto& a, const auto& b) { return a.time < b.time; };
    for (const PersonGroup& group : index.groups) {
        if (group.events > 1 && (!bucket_count || group.events < bucket_threshold)) {
            auto begin = result.events.begin() + group.event_end;
            std::sort(begin, begin + group.events, earlier);
        }

    }
    result.groups = std::move(index.groups);
    return result;
}

struct Totals {
    int64_t credit = 0, purchasing = 0, revenue = 0, assist = 0;

    void add(const Totals& other) {
        credit += other.credit;
        purchasing += other.purchasing;
        revenue += other.revenue;
        assist += other.assist;
    }
};

class CampaignDays {
    int32_t first_;
    uint64_t campaigns_;
    std::vector<Totals> dense_;
    std::unordered_map<uint64_t, Totals> sparse_;

public:
    CampaignDays(Time start, Time end, size_t campaigns)
        : first_(epoch_day(start)), campaigns_(campaigns) {
        const uint64_t days = epoch_day(end - 1) - first_ + 1;
        if (days * campaigns_ <= 262144) dense_.resize(days * campaigns_);
    }

    Totals& get(uint32_t campaign, int32_t day) {
        const uint64_t id = static_cast<uint64_t>(day - first_) * campaigns_ + campaign;
        return dense_.empty() ? sparse_[id] : dense_[id];
    }

    template<class Visit>
    void each(Visit visit) const {
        auto emit = [&](uint64_t id, const Totals& total) {
            if (total.credit || total.assist) {
                visit(static_cast<uint32_t>(id % campaigns_),
                      first_ + static_cast<int32_t>(id / campaigns_), total);
            }
        };
        for (size_t i = 0; i < dense_.size(); ++i) emit(i, dense_[i]);
        for (const auto& item : sparse_) emit(item.first, item.second);
    }
};

struct Session {
    Time start;
    int64_t revenue = 0;
    size_t day_end = 0;
};

class PersonDays {
    Keys& keys_;
    const Catalog& catalog_;
    BatchWriter<3>& writer_;
    std::vector<int64_t> categories_;
    std::vector<uint32_t> touched_;
    int64_t sessions_ = 0, events_ = 0, revenue_ = 0;

public:
    PersonDays(Keys& keys, const Catalog& catalog, BatchWriter<3>& writer)
        : keys_(keys), catalog_(catalog), writer_(writer), categories_(catalog.categories.size()) {}

    void start_session() { ++sessions_; }
    void add_events(size_t count) { events_ += count; }

    void event(const TimedEvent& event) {
        ++events_;
        if (event.cents != 0) {
            int64_t& category = categories_[event.category];
            if (category == 0) touched_.push_back(event.category);
            category += event.cents;
            revenue_ += event.cents;
        }
    }

    void flush(Key user, int32_t day) {
        Key best = 0;
        int64_t best_revenue = 0;
        for (uint32_t c : touched_) {
            const Key name = catalog_.categories[c];
            const int64_t total = categories_[c];
            if (total > best_revenue || (total == best_revenue && keys_.less(name, best))) {
                best = name;
                best_revenue = total;
            }
            categories_[c] = 0;
        }
        char user_buffer[9], best_buffer[9];
        writer_.add(keys_.text(user, user_buffer), keys_.text(best, best_buffer), best_revenue != 0,
                    day, {sessions_, events_, revenue_});
        touched_.clear();
        sessions_ = events_ = revenue_ = 0;
    }
};

struct Attribution {
    CampaignDays& totals;
    Keys& keys;
    static constexpr size_t mask_capacity = 64;
    std::vector<size_t> covered, touched;
    std::vector<uint64_t> masks;

    Attribution(CampaignDays& totals, Keys& keys, size_t campaigns)
        : totals(totals), keys(keys), covered(campaigns), masks(campaigns) {}

    void add_credit(const Session& session, const TimedClick* best) {
        const uint32_t campaign = best && best->time >= session.start - week_ms ? best->campaign : 0;
        Totals& day = totals.get(campaign, epoch_day(session.start));
        ++day.credit;
        day.purchasing += session.revenue > 0;
        day.revenue += session.revenue;
    }

    static uint64_t range_mask(size_t begin, size_t end) {
        const uint64_t prefix = end == mask_capacity ? UINT64_MAX : ((uint64_t{1} << end) - 1);
        return prefix & (UINT64_MAX << begin);
    }

    void small(const std::vector<Session>& sessions, const std::vector<TimedClick>& clicks,
               size_t begin, size_t end) {
        std::array<const TimedClick*, mask_capacity> latest;
        std::fill_n(latest.begin(), sessions.size(), nullptr);
        const Time first_start = sessions.front().start;
        const Time last_start = sessions.back().start;
        for (; begin < end; ++begin) {
            const TimedClick& click = clicks[begin];
            const Time last_eligible = click.time + week_ms;
            if (click.time > last_start || last_eligible < first_start) continue;
            const auto lo = click.time <= first_start ? sessions.begin() :
                std::lower_bound(sessions.begin(), sessions.end(), click.time,
                    [](const Session& session, Time time) { return session.start < time; });
            if (lo->start > last_eligible) continue;
            const auto hi = last_eligible >= last_start ? sessions.end() :
                std::upper_bound(lo, sessions.end(), last_eligible,
                    [](Time time, const Session& session) { return time < session.start; });
            const size_t first = lo - sessions.begin();
            const size_t stop = hi - sessions.begin();

            uint64_t& mask = masks[click.campaign];
            if (mask == 0) touched.push_back(click.campaign);
            mask |= range_mask(first, stop);

            const TimedClick*& winner = latest[first];
            if (!winner || click.time > winner->time ||
                (click.time == winner->time && click_id_less(click, *winner, keys))) winner = &click;
        }

        const TimedClick* best = nullptr;
        for (size_t i = 0; i < sessions.size(); ++i) {
            // A later first eligible index implies a strictly later click time.
            if (latest[i]) best = latest[i];
            add_credit(sessions[i], best);
        }
        if (touched.empty()) return;
        std::array<uint64_t, mask_capacity> day_masks;
        for (size_t i = 0; i < sessions.size();) {
            const int32_t day = epoch_day(sessions[i].start);
            size_t j = i + 1;
            while (j < sessions.size() && epoch_day(sessions[j].start) == day) ++j;
            std::fill(day_masks.begin() + i, day_masks.begin() + j, range_mask(i, j));
            i = j;
        }
        for (size_t campaign : touched) {
            uint64_t mask = masks[campaign];
            while (mask) {
                const size_t first = __builtin_ctzll(mask);
                const uint64_t day_mask = day_masks[first];
                totals.get(static_cast<uint32_t>(campaign), epoch_day(sessions[first].start)).assist +=
                    __builtin_popcountll(mask & day_mask);
                mask &= ~day_mask;
            }
            masks[campaign] = 0;
        }
        touched.clear();
    }

    void credit(const std::vector<Session>& sessions, const std::vector<TimedClick>& clicks,
                size_t begin, size_t end) {
        const TimedClick* best = nullptr;
        for (const Session& session : sessions) {
            while (begin < end && clicks[begin].time <= session.start) {
                const TimedClick& click = clicks[begin++];
                if (!best || click.time > best->time || click_id_less(click, *best, keys)) best = &click;
            }
            add_credit(session, best);
        }
    }

    void assist(std::vector<Session>& sessions, const std::vector<TimedClick>& clicks,
                size_t begin, size_t end) {
        if (begin == end) return;
        for (size_t i = sessions.size(); i-- > 0;) {
            sessions[i].day_end = i + 1 < sessions.size() &&
                epoch_day(sessions[i].start) == epoch_day(sessions[i + 1].start)
                ? sessions[i + 1].day_end : i + 1;
        }
        size_t lo = 0, hi = 0;
        for (; begin < end; ++begin) {
            const TimedClick& click = clicks[begin];
            while (lo < sessions.size() && sessions[lo].start < click.time) ++lo;
            if (lo == sessions.size()) break;
            while (hi < sessions.size() && sessions[hi].start <= click.time + week_ms) ++hi;
            size_t& past_end = covered[click.campaign];
            size_t i = std::max(lo, past_end);
            if (i >= hi) continue;
            if (!past_end) touched.push_back(click.campaign);
            past_end = hi;
            // Earlier clicks from this campaign can only cover a prefix of [lo, hi).
            while (i < hi) {
                const size_t j = std::min(hi, sessions[i].day_end);
                totals.get(click.campaign, epoch_day(sessions[i].start)).assist += j - i;
                i = j;
            }
        }
        for (size_t campaign : touched) covered[campaign] = 0;
        touched.clear();
    }
};

struct TimeBucket {
    Time first = 0, last = 0;
    size_t events = 0, purchase = 0;
};

struct BucketPurchase {
    size_t previous;
    uint32_t cents, category;
};

void summarize(PersonPartition& rows, Keys& keys, const Catalog& catalog,
               BatchWriter<3>& writer, CampaignDays& totals, Time start, size_t bucket_count) {
    PersonDays days(keys, catalog, writer);
    Attribution attribution(totals, keys, catalog.names.size());
    std::vector<Session> sessions;
    std::vector<TimeBucket> buckets;
    std::vector<size_t> occupied;
    std::vector<BucketPurchase> purchases;
    for (const PersonGroup& group : rows.groups) {
        const Key user = unmix_key(group.person);
        const size_t event_end = group.event_end + group.events;
        Time previous = 0;
        int32_t day = 0;
        const auto begin_segment = [&](Time first, Time last) {
            if (sessions.empty() || first - previous > session_gap) {
                const int32_t next = epoch_day(first);
                if (!sessions.empty() && next != day) days.flush(user, day);
                day = next;
                days.start_session();
                sessions.push_back({first});
            }
            previous = last;
        };
        if (bucket_count && group.events >= bucket_threshold) {
            if (buckets.empty()) buckets.resize(bucket_count);
            for (size_t i = group.event_end; i < event_end; ++i) {
                const TimedEvent& event = rows.events[i];
                const size_t index = (event.time - start) / session_gap;
                TimeBucket& bucket = buckets[index];
                if (bucket.events == 0) {
                    bucket.first = bucket.last = event.time;
                    occupied.push_back(index);
                } else {
                    bucket.first = std::min(bucket.first, event.time);
                    bucket.last = std::max(bucket.last, event.time);
                }
                ++bucket.events;
                if (event.cents != 0) {
                    purchases.push_back({bucket.purchase, event.cents, event.category});
                    bucket.purchase = purchases.size();
                }
            }
            // All events in one bucket are connected by an interval shorter than the gap.
            std::sort(occupied.begin(), occupied.end());
            for (size_t index : occupied) {
                TimeBucket& bucket = buckets[index];
                begin_segment(bucket.first, bucket.last);
                size_t nonpurchase = bucket.events;
                for (size_t p = bucket.purchase; p != 0;) {
                    const BucketPurchase& purchase = purchases[p - 1];
                    days.event({bucket.first, purchase.cents, purchase.category});
                    sessions.back().revenue += purchase.cents;
                    --nonpurchase;
                    p = purchase.previous;
                }
                days.add_events(nonpurchase);
                bucket = {};
            }
            occupied.clear();
            purchases.clear();
        } else {
            for (size_t i = group.event_end; i < event_end; ++i) {
                const TimedEvent& event = rows.events[i];
                begin_segment(event.time, event.time);
                days.event(event);
                sessions.back().revenue += event.cents;
            }
        }
        days.flush(user, day);
        const size_t click_end = group.click_end + group.clicks;
        if (group.clicks != 0 && sessions.size() <= Attribution::mask_capacity) {
            attribution.small(sessions, rows.clicks, group.click_end, click_end);
        } else {
            if (group.clicks > 1) {
                const auto earlier = [](const TimedClick& a, const TimedClick& b) { return a.time < b.time; };
                std::sort(rows.clicks.begin() + group.click_end, rows.clicks.begin() + click_end, earlier);
            }
            attribution.credit(sessions, rows.clicks, group.click_end, click_end);
            attribution.assist(sessions, rows.clicks, group.click_end, click_end);
        }
        sessions.clear();
    }
}

class Timer {
    std::chrono::steady_clock::time_point start_ = std::chrono::steady_clock::now();
public:
    void stage(const char* name) {
        const auto now = std::chrono::steady_clock::now();
        std::cerr << name << " " << std::chrono::duration<double>(now - start_).count() << " s\n";
        start_ = now;
    }
};

template<class Layout>
int process(const fs::path& data, const fs::path& output, Time start, Time end, size_t ncpu,
            Inputs& inputs, Keys& keys, const Catalog& catalog, Timer& timer, const Layout& layout) {
    Links links;
    timer.stage("catalog");

    std::vector<Chunk> chunks;
    load_chunks(inputs, data / "events", true, chunks);
    load_chunks(inputs, data / "clicks", false, chunks);
    std::sort(chunks.begin(), chunks.end(), [](const Chunk& a, const Chunk& b) {
        if (a.events != b.events) return a.events;
        return a.size > b.size;
    });
    std::promise<void> links_built;
    const auto links_ready = links_built.get_future().share();
    std::vector<WorkerInput<Layout>> workers(ncpu);
    std::vector<ChunkReader> readers(ncpu);
    parallel(ncpu, chunks.size() + 1, [&](size_t worker, size_t task) {
        if (task == 0) {
            try {
                std::array<Text, 2> c;
                inputs.folder(data / "links", [&](Text bytes, const fs::path&) {
                    lines(bytes.substr(next_line(bytes, 0)), [&](Text row) {
                        if (fields(row, c) && !c[0].empty() && !c[1].empty()) {
                            links.join(keys.encode(c[0]), keys.encode(c[1]), keys);
                        }
                    });
                });
                links.finish();
                links_built.set_value();
            } catch (...) {
                links_built.set_exception(std::current_exception());
                throw;
            }
            return;
        }
        const Chunk& chunk = chunks[task - 1];
        const Text bytes = readers[worker].read(chunk);
        if (chunk.events) {
            event_chunk(bytes, chunk.mapped, keys, catalog, workers[worker], worker, start, end, layout);
        } else {
            links_ready.get();
            click_chunk(bytes, chunk.mapped, keys, catalog, links, workers[worker], start, end);
        }
    });
    keys.freeze();
    timer.stage("links and parse");

    // The two streams have disjoint decoded ID lengths.
    parallel(ncpu, partitions * 2, [&](size_t worker, size_t task) {
        if (task < partitions) deduplicate<false>(task, workers, workers[worker], links, layout);
        else deduplicate<true>(task - partitions, workers, workers[worker], links, layout);
    });
    for (auto& worker : workers) {
        worker.candidate_storage.release();
        std::vector<WideEvents::Candidate>().swap(worker.extra);
    }
    timer.stage("event dedup");

    std::vector<size_t> order(partitions);
    std::iota(order.begin(), order.end(), 0);
    std::array<size_t, partitions> sizes{};
    for (size_t part : order) {
        for (const auto& worker : workers) {
            sizes[part] += worker.events[part].size() + worker.clicks[part].size();
        }
    }
    std::sort(order.begin(), order.end(), [&](size_t a, size_t b) { return sizes[a] > sizes[b]; });
    std::vector<std::unique_ptr<BatchWriter<3>>> writers;
    std::vector<CampaignDays> counts;
    const fs::path user_output = output / "user_daily";
    const fs::path campaign_output = output / "campaign_daily";
    fs::create_directories(user_output);
    fs::create_directories(campaign_output);
    for (size_t worker = 0; worker < ncpu; ++worker) {
        writers.push_back(std::make_unique<BatchWriter<3>>(user_output / ("part-" + std::to_string(worker) + ".parquet")));
        counts.emplace_back(start, end, catalog.names.size());
    }
    const size_t time_buckets = (end - start + session_gap - 1) / session_gap;
    const size_t bucket_count = time_buckets <= 10000 ? time_buckets : 0;
    parallel(ncpu, partitions, [&](size_t worker, size_t task) {
        const size_t part = order[task];
        auto rows = group_by_person(workers, part, bucket_count, layout);
        summarize(rows, keys, catalog, *writers[worker], counts[worker], start, bucket_count);
    });
    parallel(ncpu, writers.size(), [&](size_t, size_t i) { writers[i]->close(); });

    CampaignDays total(start, end, catalog.names.size());
    for (const auto& worker : counts) {
        worker.each([&](uint32_t campaign, int32_t day, const Totals& value) {
            total.get(campaign, day).add(value);
        });
    }
    BatchWriter<4> campaigns(campaign_output / "part-0.parquet");
    total.each([&](uint32_t campaign, int32_t day, const Totals& value) {
        const auto& names = catalog.names[campaign];
        char id_buffer[9], channel_buffer[9];
        campaigns.add(keys.text(names.first, id_buffer), keys.text(names.second, channel_buffer), true, day,
                      {value.credit, value.purchasing, value.revenue, value.assist});
    });
    campaigns.close();
    timer.stage("sort, aggregate, parquet");
    // All output is closed and workers are joined. The OS reclaims process-local storage.
    std::cerr.flush();
    if (__llvm_profile_write_file) __llvm_profile_write_file();
    std::_Exit(EXIT_SUCCESS);
}
int run(const fs::path& data, const fs::path& output, Time start, Time end, size_t ncpu) {
    if (start == bad_time || end == bad_time || end <= start || ncpu == 0) {
        throw std::runtime_error("invalid window or CPU count");
    }
    Timer timer;
    Inputs inputs;
    Keys keys;
    const Catalog catalog = load_catalog(inputs, data, keys);
    if (end - start - 1 <= INT32_MAX && catalog.categories.size() <= 256 && ncpu <= 32768) {
        const uint64_t compact_base = static_cast<uint64_t>(epoch_day(start) + ingest_day_bias - 64) << ingest_clock_bits;
        return process(data, output, start, end, ncpu, inputs, keys, catalog, timer, CompactEvents{start, compact_base});
    }
    return process(data, output, start, end, ncpu, inputs, keys, catalog, timer, WideEvents{});
}

}

int main(int argc, char** argv) {
    try {
        if (argc != 1 && argc != 3) throw std::runtime_error("expected optional data and output directories");
        const auto environment = [](const char* name, const char* fallback) {
            const char* value = std::getenv(name);
            return value ? value : fallback;
        };
        const shopping::fs::path data = argc == 3 ? argv[1] : environment("DATA_DIR", "/data");
        const shopping::fs::path output = argc == 3 ? argv[2] : environment("OUTPUT_DIR", "/output");
        const shopping::MappedFile manifest(data / "manifest.json");
        const shopping::Text bytes = manifest.bytes();
        const auto window = nlohmann::json::parse(bytes.begin(), bytes.end());
        const auto start = shopping::timestamp(window.at("window_start").get<std::string>(), true);
        const auto end = shopping::timestamp(window.at("window_end").get<std::string>(), true);
        return shopping::run(data, output, start, end, std::stoul(environment("NCPU", "8")));
    } catch (const std::exception& error) {
        std::cerr << "shopping sessions: " << error.what() << '\n';
        return 1;
    }
}
