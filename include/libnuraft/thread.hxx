#ifndef THREAD_HXX_
#define THREAD_HXX_

#if USE_CLICKHOUSE_THREADS
    #include <Common/ThreadPool.h>
    namespace nuraft
    {
        using thread = ThreadFromGlobalPool;
    }
#else
    #include <thread>
    namespace nuraft
    {
        using thread = ThreadFromGlobalPool;
    }
#endif




#endif // THREAD_HXX_
