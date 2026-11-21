#include "input.hpp"
#include "output.hpp"

#include <atomic>
#include <chrono>
#include <exception>
#include <future>
#include <iostream>
#include <memory>
#include <numeric>
#include <thread>
#include <utility>

namespace shopping {
constexpr size_t partitions = 256;
constexpr size_t chunk_bytes = 4 * 1024 * 1024;

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

    Text map(const fs::path& path) {
        files.push_back(std::make_unique<MappedFile>(path));
        return files.back()->bytes();
    }

    template<class Visit>
    void folder(const fs::path& path, Visit visit) {
        for (const auto& file : fs::directory_iterator(path)) {
            if (file.path().extension() == ".csv" && file.is_regular_file()) {
                const Text bytes = map(file.path());
                visit(bytes.substr(next_line(bytes, 0)));
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
        Key id = 0, value = 0;
    };
    std::vector<Entry> ids_ = std::vector<Entry>(1024);
    std::vector<Node> nodes_;

    static Entry& find(std::vector<Entry>& table, Key key) {
        const size_t mask = table.size() - 1;
        size_t slot = hash_key(key) & mask;
        while (table[slot].id != 0 && table[slot].id != key) slot = (slot + 1) & mask;
        return table[slot];
    }

    uint32_t add(Key key) {
        Entry* entry = &find(ids_, key);
        if (entry->id == 0) {
            if ((nodes_.size() + 1) * 4 > ids_.size() * 3) {
                std::vector<Entry> larger(ids_.size() * 2);
                for (const Entry& old : ids_) {
                    if (old.id != 0) find(larger, old.id) = old;
                }
                ids_.swap(larger);
                entry = &find(ids_, key);
            }
            if (nodes_.size() == UINT32_MAX) throw std::runtime_error("too many linked IDs");
            const auto index = static_cast<uint32_t>(nodes_.size());
            *entry = {key, index};
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
        // Reuse the ID index as the read-only canonical lookup after union-find.
        for (Entry& entry : ids_) {
            if (entry.id != 0) entry.value = nodes_[root(static_cast<uint32_t>(entry.value))].minimum;
        }
        std::vector<Node>().swap(nodes_);
    }

    Key person(Key user) const {
        const size_t mask = ids_.size() - 1;
        size_t slot = hash_key(user) & mask;
        while (ids_[slot].id != 0 && ids_[slot].id != user) slot = (slot + 1) & mask;
        return ids_[slot].id == user ? ids_[slot].value : user;
    }
};

struct Catalog {
    KeyMap<uint32_t> products, campaigns, category_ids;
    std::vector<Key> categories;
    std::vector<std::pair<Key, Key>> names;
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
        catalog.campaigns.emplace(id, catalog.names.size());
        catalog.names.emplace_back(id, keys.encode(columns[1]));
    });
    return catalog;
}

struct Event {
    Key person;
    Time time;
    uint32_t cents, category;
};

struct Candidate {
    Key id;
    Time ingest;
    Event event;
};

struct Click {
    Key person;
    Time time;
    Key id;
    uint32_t campaign;
};

struct WorkerInput {
    std::array<std::vector<Candidate>, partitions> candidates;
    std::array<std::vector<Event>, partitions> events;
    std::array<std::vector<Click>, partitions> clicks;
};

struct Chunk { Text bytes; bool events; };

void load_chunks(Inputs& inputs, const fs::path& path, bool events, std::vector<Chunk>& chunks) {
    inputs.folder(path, [&](Text bytes) {
        size_t begin = 0;
        while (begin < bytes.size()) {
            size_t end = std::min(begin + chunk_bytes, bytes.size());
            if (end != bytes.size() && bytes[end - 1] != '\n') end = next_line(bytes, end);
            chunks.push_back({bytes.substr(begin, end - begin), events});
            begin = end;
        }
    });
}

void event_chunk(Text bytes, Keys& keys, const Catalog& catalog,
                 WorkerInput& worker, Time start, Time end) {
    std::array<Text, 7> c;
    lines(bytes, [&](Text row) {
        if (!fields(row, c) || c[0].empty()) return;
        const Key id = keys.encode(c[0]);
        const Time ingest = timestamp(c[3], true);
        if (ingest == bad_time) throw std::runtime_error("invalid guaranteed ingest_ts");
        Event event{0, bad_time, 0, 0};
        const Time time = timestamp(c[2]);
        const bool purchase = c[4] == "purchase";
        if (!c[1].empty() && !c[5].empty() && (purchase || c[4] == "view" || c[4] == "cart")
            && time >= start && time < end && ingest - time <= day_ms) {
            const Time price = purchase ? digits(c[6], 10000000) : 0;
            if (!purchase || price > 0) {
                event.person = keys.encode(c[1]);
                event.time = time;
                event.cents = static_cast<uint32_t>(price);
                if (purchase) {
                    const auto found = catalog.products.find(keys.encode(c[5]));
                    if (found != catalog.products.end()) event.category = found->second;
                }
            }
        }
        worker.candidates[partition(id)].push_back({id, ingest, event});
    });
}

void click_chunk(Text bytes, Keys& keys, const Catalog& catalog, const Links& links,
                 WorkerInput& worker, Time start, Time end) {
    std::array<Text, 4> c;
    lines(bytes, [&](Text row) {
        if (!fields(row, c) || c[0].empty() || c[1].empty() || c[2].empty()) return;
        const Time time = timestamp(c[3]);
        if (time < start - week_ms || time >= end) return;
        const auto campaign = catalog.campaigns.find(keys.encode(c[2]));
        if (campaign == catalog.campaigns.end()) return;
        const Key person = links.person(keys.encode(c[1]));
        worker.clicks[partition(person)].push_back({person, time, keys.encode(c[0]), campaign->second});
    });
}

void deduplicate(size_t part, std::vector<WorkerInput>& workers, WorkerInput& writer, const Links& links) {
    size_t count = 0;
    for (const auto& worker : workers) count += worker.candidates[part].size();
    if (!count) return;
    size_t buckets = 64;
    while (buckets < 16 * count) buckets *= 2;
    std::vector<uint64_t> seen(buckets / 64), collided(buckets / 64);
    size_t repeats = 0;
    for (const auto& worker : workers) {
        for (const auto& row : worker.candidates[part]) {
            const size_t bucket = (hash_key(row.id) >> 8) & (buckets - 1);
            const uint64_t bit = uint64_t{1} << (bucket & 63);
            const size_t word = bucket >> 6;
            const uint64_t hit = seen[word] & bit;
            repeats += hit != 0;
            collided[word] |= hit;
            seen[word] |= bit;
        }
    }
    size_t slots = 1;
    // Every collided bucket contains at most twice its number of repeat visits.
    while (slots < repeats * 3 + 1) slots *= 2;
    std::vector<const Candidate*> winners(slots, nullptr);
    auto emit = [&](Event event) {
        if (event.time == bad_time) return;
        event.person = links.person(event.person);
        writer.events[partition(event.person)].push_back(event);
    };
    for (const auto& worker : workers) {
        for (const auto& row : worker.candidates[part]) {
            const uint64_t hash = hash_key(row.id) >> 8;
            const size_t bucket = hash & (buckets - 1);
            if ((collided[bucket >> 6] & (uint64_t{1} << (bucket & 63))) == 0) {
                emit(row.event);
            } else {
                size_t i = hash & (slots - 1);
                while (winners[i] && winners[i]->id != row.id) i = (i + 1) & (slots - 1);
                if (!winners[i] || row.ingest < winners[i]->ingest) winners[i] = &row;
            }
        }
    }
    for (const Candidate* winner : winners) {
        if (winner) emit(winner->event);
    }
    for (auto& worker : workers) std::vector<Candidate>().swap(worker.candidates[part]);
}

struct PersonGroup {
    Key person;
    size_t events = 0, clicks = 0, event_end = 0, click_end = 0;
};

class PersonGroups {
    std::vector<uint32_t> slots_ = std::vector<uint32_t>(256);

    size_t slot(Key person) const {
        const size_t mask = slots_.size() - 1;
        size_t index = (hash_key(person) >> 8) & mask;
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

struct PersonPartition {
    std::vector<Event> events;
    std::vector<Click> clicks;
};

PersonPartition group_by_person(std::vector<WorkerInput>& workers, size_t part) {
    PersonGroups index;
    size_t event_count = 0, click_count = 0;
    for (auto& worker : workers) {
        for (Event& event : worker.events[part]) {
            event.person = index.add(event.person);
            ++index.groups[event.person].events;
        }
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
    std::vector<uint32_t> order(index.groups.size());
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(), [&](uint32_t left, uint32_t right) {
        return index.groups[left].person < index.groups[right].person;
    });
    size_t event_offset = 0, click_offset = 0;
    for (uint32_t group_id : order) {
        PersonGroup& group = index.groups[group_id];
        group.event_end = event_offset += group.events;
        group.click_end = click_offset += group.clicks;
    }
    PersonPartition result;
    result.events.resize(event_count);
    result.clicks.resize(click_count);
    for (auto& worker : workers) {
        for (Event event : worker.events[part]) {
            PersonGroup& group = index.groups[event.person];
            event.person = group.person;
            result.events[--group.event_end] = event;
        }
        std::vector<Event>().swap(worker.events[part]);
        for (Click click : worker.clicks[part]) {
            if (click.person) {
                PersonGroup& group = index.groups[click.person - 1];
                click.person = group.person;
                result.clicks[--group.click_end] = click;
            }
        }
        std::vector<Click>().swap(worker.clicks[part]);
    }
    const auto earlier = [](const auto& a, const auto& b) { return a.time < b.time; };
    for (const PersonGroup& group : index.groups) {
        auto e = result.events.begin() + group.event_end;
        auto c = result.clicks.begin() + group.click_end;
        if (group.events > 1) std::sort(e, e + group.events, earlier);
        if (group.clicks > 1) std::sort(c, c + group.clicks, earlier);
    }
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

    void event(const Event& event) {
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
    std::vector<size_t> covered, touched;

    Attribution(CampaignDays& totals, Keys& keys, size_t campaigns)
        : totals(totals), keys(keys), covered(campaigns) {}

    void credit(const std::vector<Session>& sessions, const std::vector<Click>& clicks,
                size_t begin, size_t end) {
        const Click* best = nullptr;
        for (const Session& session : sessions) {
            while (begin < end && clicks[begin].time <= session.start) {
                const Click& click = clicks[begin++];
                if (!best || click.time > best->time || keys.less(click.id, best->id)) best = &click;
            }
            const uint32_t campaign = best && best->time >= session.start - week_ms ? best->campaign : 0;
            Totals& day = totals.get(campaign, epoch_day(session.start));
            ++day.credit;
            day.purchasing += session.revenue > 0;
            day.revenue += session.revenue;
        }
    }

    void assist(std::vector<Session>& sessions, const std::vector<Click>& clicks,
                size_t begin, size_t end) {
        if (begin == end) return;
        for (size_t i = sessions.size(); i-- > 0;) {
            sessions[i].day_end = i + 1 < sessions.size() &&
                epoch_day(sessions[i].start) == epoch_day(sessions[i + 1].start)
                ? sessions[i + 1].day_end : i + 1;
        }
        size_t lo = 0, hi = 0;
        for (; begin < end; ++begin) {
            const Click& click = clicks[begin];
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

void summarize(const std::vector<Event>& events, const std::vector<Click>& clicks,
               Keys& keys, const Catalog& catalog, BatchWriter<3>& writer, CampaignDays& totals) {
    PersonDays days(keys, catalog, writer);
    Attribution attribution(totals, keys, catalog.names.size());
    std::vector<Session> sessions;
    size_t i = 0, c = 0;
    while (i < events.size()) {
        const Key user = events[i].person;
        int32_t day = epoch_day(events[i].time);
        Time previous = events[i].time;
        days.start_session();
        sessions.push_back({previous});
        do {
            const Event& event = events[i++];
            if (event.time - previous > 1800000) {
                const int32_t next = epoch_day(event.time);
                if (next != day) {
                    days.flush(user, day);
                    day = next;
                }
                days.start_session();
                sessions.push_back({event.time});
            }
            days.event(event);
            sessions.back().revenue += event.cents;
            previous = event.time;
        } while (i < events.size() && events[i].person == user);
        days.flush(user, day);
        while (c < clicks.size() && clicks[c].person < user) ++c;
        const size_t begin = c;
        while (c < clicks.size() && clicks[c].person == user) ++c;
        attribution.credit(sessions, clicks, begin, c);
        attribution.assist(sessions, clicks, begin, c);
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

int run(const fs::path& data, const fs::path& scratch, Time start, Time end, size_t ncpu) {
    if (start == bad_time || end == bad_time || end <= start || ncpu == 0) {
        throw std::runtime_error("invalid window or CPU count");
    }
    Timer timer;
    Inputs inputs;
    Keys keys;
    const Catalog catalog = load_catalog(inputs, data, keys);
    Links links;
    timer.stage("catalog");

    std::vector<Chunk> chunks;
    load_chunks(inputs, data / "events", true, chunks);
    load_chunks(inputs, data / "clicks", false, chunks);
    std::sort(chunks.begin(), chunks.end(), [](const Chunk& a, const Chunk& b) {
        if (a.events != b.events) return a.events;
        return a.bytes.size() > b.bytes.size();
    });
    std::promise<void> links_built;
    const auto links_ready = links_built.get_future().share();
    std::vector<WorkerInput> workers(ncpu);
    parallel(ncpu, chunks.size() + 1, [&](size_t worker, size_t task) {
        if (task == 0) {
            try {
                std::array<Text, 2> c;
                inputs.folder(data / "links", [&](Text bytes) {
                    lines(bytes, [&](Text row) {
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
        if (chunk.events) {
            event_chunk(chunk.bytes, keys, catalog, workers[worker], start, end);
        } else {
            links_ready.get();
            click_chunk(chunk.bytes, keys, catalog, links, workers[worker], start, end);
        }
    });
    timer.stage("links and parse");

    parallel(ncpu, partitions, [&](size_t worker, size_t part) {
        deduplicate(part, workers, workers[worker], links);
    });
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
    for (size_t worker = 0; worker < ncpu; ++worker) {
        writers.push_back(std::make_unique<BatchWriter<3>>(scratch / ("user-" + std::to_string(worker))));
        counts.emplace_back(start, end, catalog.names.size());
    }
    parallel(ncpu, partitions, [&](size_t worker, size_t task) {
        const size_t part = order[task];
        const auto rows = group_by_person(workers, part);
        summarize(rows.events, rows.clicks, keys, catalog, *writers[worker], counts[worker]);
    });
    for (auto& writer : writers) writer->close();

    CampaignDays total(start, end, catalog.names.size());
    for (const auto& worker : counts) {
        worker.each([&](uint32_t campaign, int32_t day, const Totals& value) {
            total.get(campaign, day).add(value);
        });
    }
    BatchWriter<4> campaigns(scratch / "campaign");
    total.each([&](uint32_t campaign, int32_t day, const Totals& value) {
        const auto& names = catalog.names[campaign];
        char id_buffer[9], channel_buffer[9];
        campaigns.add(keys.text(names.first, id_buffer), keys.text(names.second, channel_buffer), true, day,
                      {value.credit, value.purchasing, value.revenue, value.assist});
    });
    campaigns.close();
    timer.stage("sort, aggregate, stage output");
    return 0;
}
}

int main(int argc, char** argv) {
    try {
        if (argc != 6) throw std::runtime_error("expected data, scratch, window start, window end and NCPU");
        return shopping::run(argv[1], argv[2], shopping::timestamp(argv[3], true),
                             shopping::timestamp(argv[4], true), std::stoul(argv[5]));
    } catch (const std::exception& error) {
        std::cerr << "shopping sessions: " << error.what() << '\n';
        return 1;
    }
}
