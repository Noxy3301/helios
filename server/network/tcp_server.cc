#include "tcp_server.hh"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sched.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "../../common/log.h"

namespace {

// HELIOS_TRANSPORT=reactor selects the epoll multi-reactor mode; anything
// else (including unset) keeps the thread-per-connection legacy path.
bool reactor_transport_enabled() {
  static const bool value = [] {
    const char *v = std::getenv("HELIOS_TRANSPORT");
    return v != nullptr && std::string(v) == "reactor";
  }();
  return value;
}

// One reactor's pin target. package_id/core_id are -1 when the topology
// source wasn't used (fallback path), where only `cpu` is meaningful.
struct CoreInfo {
  int cpu = -1;
  int package_id = -1;
  int core_id = -1;
};

// Reads one integer sysfs topology attribute for `cpu`. Returns false if the
// file is missing or its content isn't parsable.
bool read_topology_attr(int cpu, const char *attr, int &out) {
  std::ifstream f("/sys/devices/system/cpu/cpu" + std::to_string(cpu) + "/topology/" + attr);
  if (!f.is_open()) return false;
  f >> out;
  return !f.fail();
}

// Enumerates one representative CPU per physical core visible to this
// process. Groups the CPUs in the process affinity mask by
// (physical_package_id, core_id) and keeps the lowest cpu number per group;
// `out` ends up sorted by (package_id, core_id) for deterministic pinning.
// Returns false (leaving `out` unspecified) if the affinity mask or the
// sysfs topology files can't be read.
bool detect_physical_cores(std::vector<CoreInfo> &out) {
  cpu_set_t mask;
  CPU_ZERO(&mask);
  if (sched_getaffinity(0, sizeof(mask), &mask) != 0) return false;

  // (package_id, core_id) -> lowest cpu number seen for that core.
  std::map<std::pair<int, int>, int> core_to_cpu;
  for (int cpu = 0; cpu < CPU_SETSIZE; cpu++) {
    if (!CPU_ISSET(cpu, &mask)) continue;
    int package_id, core_id;
    if (!read_topology_attr(cpu, "physical_package_id", package_id) ||
        !read_topology_attr(cpu, "core_id", core_id)) {
      return false;
    }
    auto key = std::make_pair(package_id, core_id);
    auto it = core_to_cpu.find(key);
    if (it == core_to_cpu.end()) {
      core_to_cpu.emplace(key, cpu);
    } else if (cpu < it->second) {
      it->second = cpu;
    }
  }
  if (core_to_cpu.empty()) return false;

  out.clear();
  out.reserve(core_to_cpu.size());
  for (const auto &entry : core_to_cpu) {
    out.push_back(CoreInfo{entry.second, entry.first.first, entry.first.second});
  }
  return true;
}

// Per-reactor pin assignment, plus whether it came from real topology or the
// hardware_concurrency() fallback (affects how the pin table is logged).
struct ReactorPlan {
  std::vector<CoreInfo> pins;
  bool used_topology = false;
};

// Full-string checked parse of HELIOS_REACTOR_COUNT. Refuses startup on
// anything that doesn't parse exactly to a positive integer, the same
// refusal-over-silent-fallback pattern the other env knobs use (see
// database_manager.cc): a mistyped count must never silently run under a
// reactor count the caller didn't ask for.
unsigned parse_reactor_count_env(const char *raw) {
  const std::string_view input(raw);
  unsigned long parsed = 0;
  const auto [end, error] = std::from_chars(input.data(), input.data() + input.size(), parsed, 10);
  const bool consumed_all = end == input.data() + input.size();
  if (input.empty() || error != std::errc{} || !consumed_all || parsed == 0 ||
      parsed > static_cast<unsigned long>(UINT32_MAX)) {
    LOG_FATAL("Invalid HELIOS_REACTOR_COUNT='%s': expected a positive integer", raw);
  }
  return static_cast<unsigned>(parsed);
}

// Reactor count defaults to one per physical core (HELIOS_REACTOR_COUNT
// overrides for experiments). Logs the derivation and the full pin table as
// "Reactor pin:" lines.
ReactorPlan build_reactor_plan() {
  ReactorPlan plan;
  std::vector<CoreInfo> representatives;
  plan.used_topology = detect_physical_cores(representatives);

  unsigned default_count;
  unsigned fallback_hw = 0;
  if (plan.used_topology) {
    default_count = static_cast<unsigned>(representatives.size());
  } else {
    LOG_WARNING("Reactor pin: physical core topology unavailable "
                "(sched_getaffinity or sysfs topology files unreadable); "
                "falling back to hardware_concurrency() pinning");
    fallback_hw = std::thread::hardware_concurrency();
    if (fallback_hw == 0) fallback_hw = 1;
    default_count = fallback_hw;
  }

  unsigned reactor_count = default_count;
  if (const char *v = std::getenv("HELIOS_REACTOR_COUNT")) {
    reactor_count = parse_reactor_count_env(v);
    // An override above the maximum would silently double- or triple-book
    // physical cores through the i % size wrap below; clamp and warn instead.
    if (reactor_count > default_count) {
      LOG_WARNING("Reactor pin: HELIOS_REACTOR_COUNT=%u exceeds the %s maximum of %u; clamping",
                  reactor_count, plan.used_topology ? "physical-core" : "hardware_concurrency",
                  default_count);
      reactor_count = default_count;
    } else if (reactor_count != default_count) {
      LOG_WARNING("Reactor pin: HELIOS_REACTOR_COUNT=%u overrides default=%u (source=%s)",
                  reactor_count, default_count, plan.used_topology ? "topology" : "fallback");
    }
  }

  plan.pins.reserve(reactor_count);
  for (unsigned i = 0; i < reactor_count; i++) {
    if (plan.used_topology) {
      plan.pins.push_back(representatives[i % representatives.size()]);
    } else {
      CoreInfo c;
      c.cpu = static_cast<int>(i % fallback_hw);
      plan.pins.push_back(c);
    }
  }

  if (plan.used_topology) {
    LOG_INFO("Reactor pin: summary reactors=%u physical_cores=%u source=topology",
             reactor_count, default_count);
  } else {
    LOG_INFO("Reactor pin: summary reactors=%u physical_cores=unknown source=fallback",
             reactor_count);
  }
  for (unsigned i = 0; i < plan.pins.size(); i++) {
    const CoreInfo &c = plan.pins[i];
    if (plan.used_topology) {
      LOG_INFO("Reactor pin: reactor=%u cpu=%d package=%d core=%d",
               i, c.cpu, c.package_id, c.core_id);
    } else {
      LOG_INFO("Reactor pin: reactor=%u cpu=%d package=-1 core=-1 (fallback)", i, c.cpu);
    }
  }
  return plan;
}

}  // namespace

TcpServer::TcpServer(uint16_t port) : port_(port) {}

void TcpServer::run() {
  LOG_INFO("Starting server on port %d", port_);

  int server_socket;
  if (!setup_and_listen(server_socket)) {
    return;
  }

  LOG_INFO("Server listening on port %d", port_);
  if (reactor_transport_enabled()) {
    accept_clients_reactor(server_socket);
  } else {
    accept_clients(server_socket);
  }
  close(server_socket);
}

bool TcpServer::setup_and_listen(int &server_socket) {
  // Create socket
  server_socket = socket(AF_INET, SOCK_STREAM, 0);
  if (server_socket < 0) {
    int err = errno;
    LOG_ERROR("Failed to create socket: %s (errno=%d)", std::strerror(err), err);
    return false;
  }

  // Set SO_REUSEADDR option
  int reuse = 1;
  if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "Failed to set SO_REUSEADDR" << std::endl;
    close(server_socket);
    return false;
  }

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(port_);

  // Bind socket
  if (bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    std::cerr << "Failed to bind socket" << std::endl;
    close(server_socket);
    return false;
  }

  // Listen for connections
  if (listen(server_socket, 128) < 0) {
    std::cerr << "Failed to listen on socket" << std::endl;
    close(server_socket);
    return false;
  }

  return true;
}

void TcpServer::accept_clients(int server_socket) {
  static std::atomic<int> active_connections{0};
  while (true) {
    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);

    int client_socket = accept(server_socket, (struct sockaddr *)&client_addr, &client_addr_len);
    if (client_socket < 0) {
      int err = errno;
      LOG_ERROR("Failed to accept client connection: %s (errno=%d)",
                std::strerror(err), err);
      if (err == EINTR) {
        continue;  // retry on interrupt
      }
      // Sleep briefly to avoid busy loop on persistent failure conditions
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }

    // Disable Nagle's algorithm for low-latency RPC
    int flag = 1;
    setsockopt(client_socket, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    // Hand off each client to a dedicated thread
    auto client_ip = std::string(inet_ntoa(client_addr.sin_addr));
    int now_active = ++active_connections;
    LOG_INFO("Accepted connection fd=%d from %s (active=%d)",
             client_socket, client_ip.c_str(), now_active);

    std::thread([this, client_socket, client_ip]() {
      // Process the client in this thread
      handle_client(client_socket);
      // Ensure socket is closed when done
      int fd = client_socket;
      close(client_socket);
      int left = --active_connections;
      LOG_INFO("Closed connection fd=%d (%s) (active=%d)", fd, client_ip.c_str(), left);
    }).detach();
  }
}

void TcpServer::accept_clients_reactor(int server_socket) {
  ReactorPlan plan = build_reactor_plan();
  unsigned reactor_count = static_cast<unsigned>(plan.pins.size());

  std::vector<std::unique_ptr<Reactor>> reactors;
  reactors.reserve(reactor_count);
  for (unsigned i = 0; i < reactor_count; i++) {
    reactors.push_back(std::make_unique<Reactor>(
        static_cast<int>(i), plan.pins[i].cpu,
        [this](MessageType t, std::string_view payload) { return classify_rpc(t, payload); },
        [this](int fd, std::string primer) { spawn_legacy_thread(fd, std::move(primer)); }));
  }

  while (true) {
    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);

    int client_socket = accept(server_socket, (struct sockaddr *)&client_addr, &client_addr_len);
    if (client_socket < 0) {
      int err = errno;
      LOG_ERROR("Failed to accept client connection: %s (errno=%d)",
                std::strerror(err), err);
      if (err == EINTR) {
        continue;  // retry on interrupt
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }

    // Disable Nagle's algorithm for low-latency RPC, same as legacy mode.
    int flag = 1;
    setsockopt(client_socket, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    // Reactor mode reads/writes are all non-blocking; migration temporarily
    // restores blocking mode for the fd it hands off.
    int flags = fcntl(client_socket, F_GETFL, 0);
    if (flags >= 0) fcntl(client_socket, F_SETFL, flags | O_NONBLOCK);

    auto client_ip = std::string(inet_ntoa(client_addr.sin_addr));

    // Assign to the reactor with the fewest connections.
    Reactor *target = reactors[0].get();
    for (auto &r : reactors) {
      if (r->connection_count() < target->connection_count()) target = r.get();
    }
    LOG_INFO("Accepted connection fd=%d from %s (reactor mode)", client_socket, client_ip.c_str());
    target->add_connection(client_socket, create_dispatcher());
  }
}

void TcpServer::spawn_legacy_thread(int client_socket, std::string primer) {
  LOG_INFO("Migrating connection fd=%d to a dedicated legacy thread (%zu primed bytes)",
           client_socket, primer.size());
  std::thread([this, client_socket, primer = std::move(primer)]() mutable {
    handle_client(client_socket, std::move(primer));
    close(client_socket);
    LOG_INFO("Closed migrated connection fd=%d", client_socket);
  }).detach();
}
