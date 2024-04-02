#ifndef THREAD_HXX_
#define THREAD_HXX_

#include <Common/ThreadPool.h>
#include <thread>

namespace nuraft
{
#if USE_CLICKHOUSE_THREADS
    using thread = ThreadFromGlobalPool;
#else
    using thread = std::thread;
#endif

}


#endif // THREAD_HXX_
