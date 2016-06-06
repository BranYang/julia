// This file is a part of Julia. License is MIT: http://julialang.org/license

#include "llvm-version.h"
#include "platform.h"
#include "options.h"

#ifdef USE_MCJIT
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include "julia.h"
#include "julia_internal.h"

#ifdef LLVM37
#ifndef LLVM38
#  include <llvm/ExecutionEngine/Orc/ObjectLinkingLayer.h>
#endif
#ifdef _OS_LINUX_
#  include <sys/syscall.h>
#  ifdef __NR_memfd_create
#    include <linux/memfd.h>
#  endif
#endif
#ifndef _OS_WINDOWS_
#  include <sys/mman.h>
#  include <sys/stat.h>
#  include <fcntl.h>
#  include <unistd.h>
#  if defined(_OS_DARWIN_) && !defined(MAP_ANONYMOUS)
#    define MAP_ANONYMOUS MAP_ANON
#  endif
#endif
#ifdef _OS_FREEBSD_
#  include <sys/types.h>
#endif

namespace {

// static void print_to_tty(const char *fmt, ...)
// {
//     char *env = getenv("JULIA_DEBUG_CGMEMMGR");
//     if (!env || !*env)
//         return;
//     va_list args;
//     va_start(args, fmt);
//     static int fd = -1;
//     if (fd == -1)
//         fd = open("/dev/tty", O_RDWR);
//     vdprintf(fd == -1 ? 2 : fd, fmt, args);
//     va_end(args);
// }

static size_t get_block_size(size_t size)
{
    return (size > jl_page_size * 256 ? LLT_ALIGN(size, jl_page_size) :
            jl_page_size * 256);
}

// Wrapper function to mmap/munmap/mprotect pages...
static void *map_anon_page(size_t size)
{
#ifdef _OS_WINDOWS_
    char *mem = (char*)VirtualAlloc(NULL, size + jl_page_size,
                                    MEM_COMMIT, PAGE_READWRITE);
    if (!mem) {
        jl_safe_printf("%s failed, %x, %lld\n", __func__,
                       (unsigned)GetLastError(), (long long)size);
        abort();
    }
    mem = (char*)LLT_ALIGN(uintptr_t(mem), jl_page_size);
#else
    void *mem = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                     MAP_NORESERVE | MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    // print_to_tty("%s: %p, %lld\n", __func__, mem, (long long)size);
    assert(mem != MAP_FAILED);
#endif
    return mem;
}

static void unmap_page(void *ptr, size_t size)
{
    // print_to_tty("%s: %p, %lld\n", __func__, ptr, (long long)size);
#ifdef _OS_WINDOWS_
    VirtualFree(ptr, size, MEM_DECOMMIT);
#else
    munmap(ptr, size);
#endif
}

#ifdef _OS_WINDOWS_
enum class Prot : int {
    None = PAGE_NOACCESS,
    RW = PAGE_READWRITE,
    RX = PAGE_EXECUTE,
    RO = PAGE_READONLY
};
static void protect_page(void *ptr, size_t size, Prot flags)
{
    DWORD old_prot;
    if (!VirtualProtect(ptr, size, (DWORD)flags, &old_prot)) {
        jl_safe_printf("%s failed, %x, %p, %lld, %d\n", __func__,
                       (unsigned)GetLastError(), ptr, (long long)size,
                       (int)flags);
        abort();
    }
}
#else
enum class Prot : int {
    None = PROT_NONE,
    RW = PROT_READ | PROT_WRITE,
    RX = PROT_READ | PROT_EXEC,
    RO = PROT_READ
};
static void protect_page(void *ptr, size_t size, Prot flags)
{
    int ret = mprotect(ptr, size, (int)flags);
    if (ret != 0) {
        perror(__func__);
        abort();
    }
    // print_to_tty("%s: %p, %lld, %d, %d\n",
    //              __func__, ptr, (long long)size, (int)flags, ret);
}
#endif

#ifndef _OS_WINDOWS_
static bool check_fd_or_close(int fd)
{
    if (fd == -1)
        return false;
    // This can fail due to `noexec` mount option ....
    fcntl(fd, F_SETFD, FD_CLOEXEC);
    fchmod(fd, S_IRWXU);
    ftruncate(fd, jl_page_size);
    void *ptr = mmap(nullptr, jl_page_size, PROT_READ | PROT_EXEC,
                     MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) {
        close(fd);
        return false;
    }
    munmap(ptr, jl_page_size);
    return true;
}
#endif

// For shared mapped region
static intptr_t get_anon_hdl(void)
{
#ifndef _OS_WINDOWS_
    int fd = -1;

    // Linux and FreeBSD can create an anonymous fd without touching the
    // file system.
#  ifdef __NR_memfd_create
    fd = syscall(__NR_memfd_create, "julia-codegen", MFD_CLOEXEC);
    // print_to_tty("%s: memfd %d\n", __func__, fd);
    if (check_fd_or_close(fd))
        return fd;
#  endif
#  ifdef _OS_FREEBSD_
    fd = shm_open(SHM_ANON, O_RDWR, S_IRWXU);
    if (check_fd_or_close(fd))
        return fd;
#  endif
    char shm_name[] = "julia-codegen-0123456789-0123456789/tmp///";
    pid_t pid = getpid();
    // `shm_open` can't be mapped exec on mac
#  ifndef _OS_DARWIN_
    do {
        snprintf(shm_name, sizeof(shm_name),
                 "julia-codegen-%d-%d", (int)pid, rand());
        fd = shm_open(shm_name, O_RDWR | O_CREAT | O_EXCL, S_IRWXU);
        // print_to_tty("%s: shm %d\n", __func__, fd);
        if (check_fd_or_close(fd)) {
            shm_unlink(shm_name);
            return fd;
        }
    } while (errno == EEXIST);
#  endif
    FILE *tmpf = tmpfile();
    if (tmpf) {
        fd = dup(fileno(tmpf));
        fclose(tmpf);
        if (check_fd_or_close(fd)) {
            return fd;
        }
    }
    snprintf(shm_name, sizeof(shm_name),
             "/tmp/julia-codegen-%d-XXXXXX", (int)pid);
    fd = mkstemp(shm_name);
    // print_to_tty("%s: mkstemp %d\n", __func__, fd);
    if (check_fd_or_close(fd)) {
        unlink(shm_name);
        return fd;
    }
    return -1;
#else // _OS_WINDOWS_
    // As far as I can tell `CreateFileMapping` cannot be resized on windows.
    // As a work around, we create multiple memory objects each of 512MB.
    // The handle returned is a poor man array of handles
    // (ptr[-1] == size, ptr[-2] == capacity).
    // The lowest 29 bits of the offset is the offset in the memory object
    // and the higher bits are the index into the handle array.
    // This limits the maximum block size to be 512MB.
    // Hopefully this is enough and we don't need more fancy page management...
    intptr_t *handles = 2 + (intptr_t*)malloc(sizeof(void*) * 128);
    handles[-2] = 126;
    handles[-1] = 0;
    return (intptr_t)handles;
#endif // _OS_WINDOWS_
}

static intptr_t anon_hdl = -1;
static size_t map_offset = 0;
// Multiple of 512MB.
// Hopefully no one will set a ulimit for this to be a problem...
static size_t map_size = 0;
static jl_mutex_t shared_map_lock;

#ifdef _OS_WINDOWS_
static void *create_shared_map(size_t size, size_t offset)
{
    size_t real_offset = offset & ((1 << 29) - 1);
    size_t map_idx = offset >> 29;
    JL_LOCK_NOGC(&shared_map_lock);
    intptr_t hdl = ((intptr_t*)anon_hdl)[map_idx];
    JL_UNLOCK_NOGC(&shared_map_lock);
    void *addr = MapViewOfFile((HANDLE)hdl, FILE_MAP_ALL_ACCESS,
                               0, uint32_t(real_offset), size);
    if (!addr) {
        jl_safe_printf("%s failed, %x: size %lld, offset %lld\n",
                       __func__, (unsigned)GetLastError(), (long long)size,
                       (long long)offset);
        abort();
    }
    return addr;
}

static intptr_t init_shared_map()
{
    anon_hdl = get_anon_hdl();
    if (anon_hdl == -1)
        return -1;
    map_offset = 0;
    map_size = 512 * 1024 * 1024;
    intptr_t hdl = (intptr_t)CreateFileMapping(INVALID_HANDLE_VALUE, NULL,
                                               PAGE_EXECUTE_READWRITE,
                                               0, map_size, NULL);
    if (!hdl)
        return -1;
    ((intptr_t*)anon_hdl)[0] = hdl;
    ((intptr_t*)anon_hdl)[-1] = 1;
    return anon_hdl;
}

static void *alloc_shared_page(size_t size, size_t *offset, bool exec)
{
    assert(size < 512 * 1024 * 1024);
    assert(size % jl_page_size == 0);
    JL_LOCK_NOGC(&shared_map_lock);
    intptr_t *handles = (intptr_t*)anon_hdl;
    size_t off = map_offset;
    size_t new_off = off + size;
    intptr_t hdl;
    if (new_off > map_size) {
        off = map_size;
        new_off = off + size;
        size_t nmap = handles[-1];
        size_t avail = handles[-2];
        if (nmap >= avail) {
            handles = 2 + (intptr_t*)realloc(handles, nmap * 2 * sizeof(void*));
            anon_hdl = (intptr_t)handles;
            handles[-2] = nmap * 2;
        }
        hdl = (intptr_t)CreateFileMapping(INVALID_HANDLE_VALUE, NULL,
                                          PAGE_EXECUTE_READWRITE,
                                          0, 512 * 1024 * 1024, NULL);
        handles[nmap] = hdl;
        handles[-1] = nmap + 1;
        map_size += 512 * 1024 * 1024;
    }
    else {
        hdl = handles[handles[-1] - 1];
    }
    map_offset = new_off;
    *offset = off;
    JL_UNLOCK_NOGC(&shared_map_lock);
    size_t real_offset = off & ((1 << 29) - 1);
    auto mode = FILE_MAP_READ | (exec ? FILE_MAP_EXECUTE : 0);
    void *addr = MapViewOfFile((HANDLE)hdl, mode,
                               0, uint32_t(real_offset), size);
    if (!addr) {
        jl_safe_printf("%s failed, %x: size %lld, offset %lld\n",
                       __func__, (unsigned)GetLastError(), (long long)size,
                       (long long)real_offset);
        abort();
    }
    return addr;
}
#else
static void *create_shared_map(size_t size, size_t offset)
{
    void *addr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED,
                      anon_hdl, offset);
    // print_to_tty("%s: %p, %lld\n", __func__, addr, (long long)size);
    assert(addr != MAP_FAILED);
    return addr;
}

static intptr_t init_shared_map()
{
    anon_hdl = get_anon_hdl();
    if (anon_hdl == -1)
        return -1;
    map_offset = 0;
    map_size = 512 * 1024 * 1024;
    int ret = ftruncate(anon_hdl, map_size);
    // print_to_tty("%s: %d, %lld\n", __func__, ret, (long long)map_size);
    if (ret != 0) {
        perror(__func__);
        abort();
    }
    return anon_hdl;
}

static void *alloc_shared_page(size_t size, size_t *offset, bool exec)
{
    assert(size % jl_page_size == 0);
    size_t off = jl_atomic_fetch_add(&map_offset, size);
    *offset = off;
    if (__unlikely(off + size > map_size)) {
        JL_LOCK_NOGC(&shared_map_lock);
        size_t old_size = map_size;
        while (off + size > map_size)
            map_size += 512 * 1024 * 1024;
        if (old_size != map_size) {
            int ret = ftruncate(anon_hdl, map_size);
            // print_to_tty("%s: %d, %lld\n", __func__, ret, (long long)map_size);
            if (ret != 0) {
                perror(__func__);
                abort();
            }
        }
        JL_UNLOCK_NOGC(&shared_map_lock);
    }
    return create_shared_map(size, off);
}
#endif

#if defined(_OS_LINUX_)
// Using `/proc/self/mem`, A.K.A. Keno's remote memory manager.

static int self_mem_fd = -1;

static int init_self_mem()
{
    int fd = open("/proc/self/mem", O_RDWR | O_SYNC | O_CLOEXEC);
    if (fd == -1)
        return -1;
    // buffer to check if write works;
    volatile uint64_t buff = 0;
    uint64_t v = 0x12345678;
    int ret = pwrite(fd, (void*)&v, sizeof(uint64_t), (uintptr_t)&buff);
    if (ret != sizeof(uint64_t) || buff != 0x12345678) {
        close(fd);
        return -1;
    }
    self_mem_fd = fd;
    return fd;
}

static void write_self_mem(void *dest, void *ptr, size_t size)
{
    while (size > 0) {
        // jl_safe_printf("%s: %p <= %p, %lld\n", __func__,
        //                dest, ptr, (long long)size);
        ssize_t ret = pwrite(self_mem_fd, ptr, size, (uintptr_t)dest);
        if (ret == size)
            return;
        if (ret == -1 && (errno == EAGAIN || errno == EINTR))
            continue;
        assert(ret < size);
        size -= ret;
        ptr = (char*)ptr + ret;
        dest = (char*)dest + ret;
    }
}
#endif

using namespace llvm;

// Allocation strategies
// * For RW data, no memory protection needed, use plain memory pool.
// * For RO data or code,
//
//   The first allocation in the page always has write address equals to
//   runtime address.
//
//   1. shared dual map
//
//       Map an (unlinked) anonymous file as memory pool.
//       After first allocation, write address points to the second map.
//       The second map is set to unreadable and unwritable in finalization.
//
//   2. private dual map
//
//       Same with above but use anonymous memory map as memory pool,
//       and use low level OS api to set up the second map.
//
//   3. copying data into RO page bypassing page protection
//
//       After first allocation, write address points to a temporary buffer.
//       Requires copying data out of the temporary buffer in finalization.

// Allocates at least 256 pages per block and keep up to 8 blocks in the free
// list. The block with the least free space is discarded when we need to
// allocate a new page.
// Unused full pages are free'd from the block before discarding so at most
// one page is wasted on each discarded blocks. There should be at most one
// block with more than 128 pages available so the discarded one must have
// less than 128 pages available and therefore at least 128 pages used.
// (Apart from fragmentation) this guarantees less one 1% of memory wasting.

// the `shared` type parameter is for Windows only....
struct Block {
    // runtime address
    char *ptr{nullptr};
    size_t total{0};
    size_t avail{0};

    Block(const Block&) = delete;
    Block &operator=(const Block&) = delete;

    Block() = default;

    void *alloc(size_t size, size_t align)
    {
        size_t aligned_avail = avail & (-align);
        if (aligned_avail < size)
            return nullptr;
        char *p = ptr + total - aligned_avail;
        avail = aligned_avail - size;
        return p;
    }
    void reset(void *addr, size_t size)
    {
        if (avail >= jl_page_size) {
            uintptr_t end = uintptr_t(ptr) + total;
            uintptr_t first_free = end - avail;
            first_free = LLT_ALIGN(first_free, jl_page_size);
            assert(first_free < end);
            unmap_page((void*)first_free, end - first_free);
        }
        ptr = (char*)addr;
        total = avail = size;
    }
};

class RWAllocator {
    static constexpr int nblocks = 8;
    Block blocks[nblocks]{};
public:
    void *alloc(size_t size, size_t align)
    {
        size_t min_size = (size_t)-1;
        int min_id = 0;
        for (int i = 0;i < nblocks && blocks[i].ptr;i++) {
            if (void *ptr = blocks[i].alloc(size, align))
                return ptr;
            if (blocks[i].avail < min_size) {
                min_size = blocks[i].avail;
                min_id = i;
            }
        }
        size_t block_size = get_block_size(size);
        blocks[min_id].reset(map_anon_page(block_size), block_size);
        return blocks[min_id].alloc(size, align);
    }
};

struct ROBlock : public Block {
    // Possible states
    // Allocation:
    // * Initial allocation: `state & InitAlloc`
    // * Followup allocation: `(state & Alloc) && !(state & InitAlloc)`
    enum State {
        // This block is has no page protection set yet.
        InitAlloc = (1 << 0),
        // There is at least one allocation in this page.
        Alloc = (1 << 1),
        // `wr_ptr` can be directly used as write address.
        WRInit = (1 << 2),
        // With `WRInit` set, whether `wr_ptr` has write permission enabled.
        WRReady = (1 << 3),
    };

    uintptr_t wr_ptr{0};
    uint32_t state{0};
    ROBlock() = default;

    void swap(ROBlock &other)
    {
        std::swap(ptr, other.ptr);
        std::swap(total, other.total);
        std::swap(avail, other.avail);
        std::swap(wr_ptr, other.wr_ptr);
        std::swap(state, other.state);
    }

    ROBlock(ROBlock &&other)
        : ROBlock()
    {
        swap(other);
    }
};

struct Allocation {
    void *wr_addr;
    void *rt_addr;
    size_t sz;
    bool relocated;
};

template<bool exec>
class ROAllocator {
protected:
    static constexpr int nblocks = 8;
    ROBlock blocks[nblocks];
    // Blocks that are done allocating but might not have all the permissions
    // set or data copied yet.
    SmallVector<ROBlock, 16> pendings;
    virtual void *get_wr_ptr(ROBlock &block, void *rt_ptr,
                             size_t size, size_t align) = 0;
    virtual ROBlock alloc_block(size_t size) = 0;
public:
    virtual ~ROAllocator() {}
    virtual void finalize()
    {
        if (exec) {
            for (auto &alloc: allocations) {
                sys::Memory::InvalidateInstructionCache(alloc.rt_addr,
                                                        alloc.sz);
            }
        }
        pendings.clear();
        allocations.clear();
    }
    // Allocations that have not been finalized yet.
    SmallVector<Allocation, 16> allocations;
    void *alloc(size_t size, size_t align)
    {
        size_t min_size = (size_t)-1;
        int min_id = 0;
        for (int i = 0;i < nblocks && blocks[i].ptr;i++) {
            auto &block = blocks[i];
            void *ptr = block.alloc(size, align);
            if (ptr) {
                void *wr_ptr;
                if (block.state & ROBlock::InitAlloc) {
                    wr_ptr = ptr;
                }
                else {
                    wr_ptr = get_wr_ptr(block, ptr, size, align);
                }
                block.state |= ROBlock::Alloc;
                // print_to_tty("%s: %p, %p, %lld\n",
                //              __func__, wr_ptr, ptr, (long long)size);
                allocations.push_back(Allocation{wr_ptr, ptr, size, false});
                return wr_ptr;
            }
            if (block.avail < min_size) {
                min_size = block.avail;
                min_id = i;
            }
        }
        size_t block_size = get_block_size(size);
        auto &block = blocks[min_id];
        auto new_block = alloc_block(block_size);
        block.swap(new_block);
        if (new_block.state) {
            pendings.push_back(std::move(new_block));
        }
        else {
            new_block.reset(nullptr, 0);
        }
        void *ptr = block.alloc(size, align);
#ifdef _OS_WINDOWS_
        block.state = ROBlock::Alloc;
        void *wr_ptr = get_wr_ptr(block, ptr, size, align);
        allocations.push_back(Allocation{wr_ptr, ptr, size, false});
        ptr = wr_ptr;
#else
        block.state = ROBlock::Alloc | ROBlock::InitAlloc;
        // print_to_tty("%s: %p, %p, %lld\n",
        //              __func__, ptr, ptr, (long long)size);
        allocations.push_back(Allocation{ptr, ptr, size, false});
#endif
        return ptr;
    }
};

template<bool exec>
class DualMapAllocator : public ROAllocator<exec> {
protected:
    void *get_wr_ptr(ROBlock &block, void *rt_ptr, size_t, size_t) override
    {
        assert((char*)rt_ptr >= block.ptr &&
               (char*)rt_ptr < (block.ptr + block.total));
        if (!(block.state & ROBlock::WRInit)) {
            block.wr_ptr = (uintptr_t)create_shared_map(block.total,
                                                        block.wr_ptr);
            block.state |= ROBlock::WRInit;
        }
        if (!(block.state & ROBlock::WRReady)) {
            protect_page((void*)block.wr_ptr, block.total, Prot::RW);
            block.state |= ROBlock::WRReady;
        }
        return (char*)rt_ptr + (block.wr_ptr - uintptr_t(block.ptr));
    }
    ROBlock alloc_block(size_t size) override
    {
        ROBlock new_block;
        // use `wr_ptr` to record offset initially
        auto ptr = alloc_shared_page(size, (size_t*)&new_block.wr_ptr, exec);
        new_block.reset(ptr, size);
        return new_block;
    }
    void finalize_block(ROBlock &block, bool reset)
    {
        if (!(block.state & ROBlock::Alloc)) {
            if ((block.state & ROBlock::WRInit) && reset)
                unmap_page((void*)block.wr_ptr, block.total);
            return;
        }
        if (block.state & ROBlock::InitAlloc) {
            // for a initial block, we need to map it to ro or rx
            assert(!(block.state & (ROBlock::WRReady | ROBlock::WRInit)));
            protect_page(block.ptr, block.total, exec ? Prot::RX : Prot::RO);
            block.state = 0;
        }
        else {
            // for other ones, we need to map the write address to none
            assert(block.state & ROBlock::WRInit);
            assert(block.state & ROBlock::WRReady);
            if (reset) {
                unmap_page((void*)block.wr_ptr, block.total);
            }
            else {
                protect_page((void*)block.wr_ptr, block.total, Prot::None);
                block.state = ROBlock::WRInit;
            }
        }
    }
public:
    DualMapAllocator()
    {
        assert(anon_hdl != -1);
    }
    void finalize() override
    {
        for (auto &block : this->blocks) {
            finalize_block(block, false);
        }
        for (auto &block : this->pendings) {
            finalize_block(block, true);
            block.reset(nullptr, 0);
        }
        ROAllocator<exec>::finalize();
    }
};

#if defined(_OS_LINUX_)
template<bool exec>
class SelfMemAllocator : public ROAllocator<exec> {
    SmallVector<Block, 16> temp_buff;
protected:
    void *get_wr_ptr(ROBlock &block, void *rt_ptr,
                     size_t size, size_t align) override
    {
        assert(!(block.state & ROBlock::InitAlloc));
        for (auto &wr_block: temp_buff) {
            if (void *ptr = wr_block.alloc(size, align)) {
                return ptr;
            }
        }
        temp_buff.emplace_back();
        Block &new_block = temp_buff.back();
        size_t block_size = get_block_size(size);
        new_block.reset(map_anon_page(block_size), block_size);
        return new_block.alloc(size, align);
    }
    ROBlock alloc_block(size_t size) override
    {
        ROBlock new_block;
        new_block.reset(map_anon_page(size), size);
        return new_block;
    }
    void finalize_block(ROBlock &block, bool reset)
    {
        if (!(block.state & ROBlock::Alloc))
            return;
        if (block.state & ROBlock::InitAlloc) {
            // for a initial block, we need to map it to ro or rx
            assert(!(block.state & (ROBlock::WRReady | ROBlock::WRInit)));
            protect_page(block.ptr, block.total, exec ? Prot::RX : Prot::RO);
            block.state = 0;
        }
    }
public:
    SelfMemAllocator()
        : ROAllocator<exec>(),
          temp_buff()
    {
        assert(self_mem_fd != -1);
    }
    void finalize() override
    {
        for (auto &block : this->blocks) {
            finalize_block(block, false);
        }
        for (auto &block : this->pendings) {
            finalize_block(block, true);
            block.reset(nullptr, 0);
        }
        for (auto &alloc : this->allocations) {
            if (alloc.rt_addr == alloc.wr_addr)
                continue;
            write_self_mem(alloc.rt_addr, alloc.wr_addr, alloc.sz);
        }
        // clear all the temp buffers except the first one
        // (we expect only one)
        bool cached = false;
        for (auto &block : temp_buff) {
            if (cached) {
                munmap(block.ptr, block.total);
                block.ptr = nullptr;
                block.total = block.avail = 0;
            }
            else {
                block.avail = block.total;
                cached = true;
            }
        }
        if (cached)
            temp_buff.resize(1);
        ROAllocator<exec>::finalize();
    }
};
#endif

class RTDyldMemoryManagerJL : public SectionMemoryManager {
    struct EHFrame {
        uint8_t *addr;
        size_t size;
    };
    RTDyldMemoryManagerJL(const RTDyldMemoryManagerJL&) = delete;
    void operator=(const RTDyldMemoryManagerJL&) = delete;
    SmallVector<EHFrame, 16> pending_eh;
    RWAllocator rw_alloc;
    ROAllocator<false> *ro_alloc;
    ROAllocator<true> *exe_alloc;

public:
    RTDyldMemoryManagerJL()
        : SectionMemoryManager(),
          pending_eh(),
          rw_alloc(),
          ro_alloc(nullptr),
          exe_alloc(nullptr)
    {
#if defined(_OS_LINUX_)
        if (!ro_alloc && init_self_mem() != -1) {
            ro_alloc = new SelfMemAllocator<false>();
            exe_alloc = new SelfMemAllocator<true>();
        }
#endif
        if (!ro_alloc && init_shared_map() != -1) {
            ro_alloc = new DualMapAllocator<false>();
            exe_alloc = new DualMapAllocator<true>();
        }
    }
    ~RTDyldMemoryManagerJL() override
    {
        delete ro_alloc;
        delete exe_alloc;
    }
    void registerEHFrames(uint8_t *Addr, uint64_t LoadAddr,
                          size_t Size) override;
    void deregisterEHFrames(uint8_t *Addr, uint64_t LoadAddr,
                            size_t Size) override;
    uint8_t *allocateCodeSection(uintptr_t Size, unsigned Alignment,
                                 unsigned SectionID,
                                 StringRef SectionName) override;
    uint8_t *allocateDataSection(uintptr_t Size, unsigned Alignment,
                                 unsigned SectionID, StringRef SectionName,
                                 bool isReadOnly) override;
#ifdef LLVM38
    void notifyObjectLoaded(RuntimeDyld &Dyld,
                            const object::ObjectFile &Obj) override;
#endif
    bool finalizeMemory(std::string *ErrMsg = nullptr) override;
    template <typename DL, typename Alloc>
    void mapAddresses(DL &Dyld, Alloc *allocator)
    {
        for (auto &alloc: allocator->allocations) {
            if (alloc.rt_addr == alloc.wr_addr || alloc.relocated)
                continue;
            alloc.relocated = true;
            Dyld.mapSectionAddress(alloc.wr_addr, (uintptr_t)alloc.rt_addr);
        }
    }
    template <typename DL>
    void mapAddresses(DL &Dyld)
    {
        if (!ro_alloc)
            return;
        mapAddresses(Dyld, ro_alloc);
        mapAddresses(Dyld, exe_alloc);
    }
#ifdef _OS_WINDOWS_
    template <typename Alloc>
    void *lookupWriteAddressFor(void *rt_addr, Alloc *allocator)
    {
        for (auto &alloc: allocator->allocations) {
            if (alloc.rt_addr == rt_addr) {
                return alloc.wr_addr;
            }
        }
        return nullptr;
    }
    void *lookupWriteAddressFor(void *rt_addr)
    {
        if (!ro_alloc)
            return rt_addr;
        if (void *ptr = lookupWriteAddressFor(rt_addr, ro_alloc))
            return ptr;
        if (void *ptr = lookupWriteAddressFor(rt_addr, exe_alloc))
            return ptr;
        return rt_addr;
    }
#endif
};

uint8_t *RTDyldMemoryManagerJL::allocateCodeSection(uintptr_t Size,
                                                    unsigned Alignment,
                                                    unsigned SectionID,
                                                    StringRef SectionName)
{
    if (exe_alloc)
        return (uint8_t*)exe_alloc->alloc(Size, Alignment);
    return SectionMemoryManager::allocateCodeSection(Size, Alignment, SectionID,
                                                     SectionName);
}

uint8_t *RTDyldMemoryManagerJL::allocateDataSection(uintptr_t Size,
                                                    unsigned Alignment,
                                                    unsigned SectionID,
                                                    StringRef SectionName,
                                                    bool isReadOnly)
{
    if (!isReadOnly)
        return (uint8_t*)rw_alloc.alloc(Size, Alignment);
    if (ro_alloc)
        return (uint8_t*)ro_alloc->alloc(Size, Alignment);
    return SectionMemoryManager::allocateDataSection(Size, Alignment, SectionID,
                                                     SectionName, isReadOnly);
}

#ifdef LLVM38
void RTDyldMemoryManagerJL::notifyObjectLoaded(RuntimeDyld &Dyld,
                                               const object::ObjectFile &Obj)
{
    if (!ro_alloc) {
        SectionMemoryManager::notifyObjectLoaded(Dyld, Obj);
        return;
    }
    mapAddresses(Dyld);
}
#endif

bool RTDyldMemoryManagerJL::finalizeMemory(std::string *ErrMsg)
{
    if (ro_alloc) {
        ro_alloc->finalize();
        exe_alloc->finalize();
        for (auto &frame: pending_eh)
            register_eh_frames(frame.addr, frame.size);
        pending_eh.clear();
        return false;
    }
    else {
        return SectionMemoryManager::finalizeMemory(ErrMsg);
    }
}

void RTDyldMemoryManagerJL::registerEHFrames(uint8_t *Addr,
                                             uint64_t LoadAddr,
                                             size_t Size)
{
    if (uintptr_t(Addr) == LoadAddr) {
        register_eh_frames(Addr, Size);
    }
    else {
        pending_eh.push_back(EHFrame{(uint8_t*)(uintptr_t)LoadAddr, Size});
    }
}

void RTDyldMemoryManagerJL::deregisterEHFrames(uint8_t *Addr,
                                               uint64_t LoadAddr,
                                               size_t Size)
{
    deregister_eh_frames((uint8_t*)LoadAddr, Size);
}

}

#ifndef LLVM38
void notifyObjectLoaded(RTDyldMemoryManager *memmgr,
                        llvm::orc::ObjectLinkingLayerBase::ObjSetHandleT H)
{
    ((RTDyldMemoryManagerJL*)memmgr)->mapAddresses(**H);
}
#endif

#ifdef _OS_WINDOWS_
void *lookupWriteAddressFor(RTDyldMemoryManager *memmgr, void *rt_addr)
{
    return ((RTDyldMemoryManagerJL*)memmgr)->lookupWriteAddressFor(rt_addr);
}
#endif

#else
typedef SectionMemoryManager RTDyldMemoryManagerJL;
#endif

RTDyldMemoryManager* createRTDyldMemoryManager()
{
    return new RTDyldMemoryManagerJL();
}
#endif
