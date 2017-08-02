// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
// Author: Ge,Jun (gejun@baidu.com)
// Date: Tue Jul 28 18:14:40 CST 2015

#include "base/time.h"
#include "base/memory/singleton_on_pthread_once.h"
#include "bvar/reducer.h"
#include "bvar/detail/sampler.h"
#include "bvar/passive_status.h"
#include "bvar/window.h"

namespace bvar {
namespace detail {

const int WARN_NOSLEEP_THRESHOLD = 2;

// Combine two circular linked list into one.
struct CombineSampler {
    void operator()(Sampler* & s1, Sampler* s2) const {
        if (s2 == NULL) {
            return;
        }
        if (s1 == NULL) {
            s1 = s2;
            return;
        }
        s1->InsertBeforeAsList(s2);
    }
};

// Call take_sample() of all scheduled samplers.
// This can be done with regular timer thread, but it's way too slow(global
// contention + log(N) heap manipulations). We need it to be super fast so that
// creation overhead of Window<> is negliable.
// The trick is to use Reducer<Sampler*, CombineSampler>. Each Sampler is
// doubly linked, thus we can reduce multiple Samplers into one cicurlarly
// doubly linked list, and multiple lists into larger lists. We create a
// dedicated thread to periodically get_value() which is just the combined
// list of Samplers. Waking through the list and call take_sample().
// If a Sampler needs to be deleted, we just mark it as unused and the
// deletion is taken place in the thread as well.
class SamplerCollector : public bvar::Reducer<Sampler*, CombineSampler> {
public:
    SamplerCollector() : _created(false), _stop(false), _cumulated_time_us(0) {
        int rc = pthread_create(&_tid, NULL, sampling_thread, this);
        if (rc != 0) {
            LOG(FATAL) << "Fail to create sampling_thread, " << berror(rc);
        } else {
            _created = true;
        }
    }
    ~SamplerCollector() {
        VLOG(99) << "SamplerCollector is quiting";
        if (_created) {
            _stop = true;
            pthread_join(_tid, NULL);
            _created = false;
        }
    }

    static double get_cumulated_time(void* arg) {
        return ((SamplerCollector*)arg)->_cumulated_time_us / 1000.0 / 1000.0;
    }
    
private:
    void run();
    
    static void* sampling_thread(void* arg) {
        ((SamplerCollector*)arg)->run();
        return NULL;
    }

private:
    bool _created;
    bool _stop;
    int64_t _cumulated_time_us;
    pthread_t _tid;
};

void SamplerCollector::run() {
    VLOG(99) << "SamplerCollector starts to run";
    base::LinkNode<Sampler> root;
    int consecutive_nosleep = 0;
    PassiveStatus<double> cumulated_time(get_cumulated_time, this);
    bvar::PerSecond<bvar::PassiveStatus<double> > usage(
            "bvar_sampler_collector_usage", &cumulated_time, 10);
    while (!_stop) {
        int64_t abstime = base::gettimeofday_us();
        Sampler* s = this->reset();
        if (s) {
            s->InsertBeforeAsList(&root);
        }
        int nremoved = 0;
        int nsampled = 0;
        for (base::LinkNode<Sampler>* p = root.next(); p != &root;) {
            // We may remove p from the list, save next first.
            base::LinkNode<Sampler>* saved_next = p->next();
            Sampler* s = p->value();
            pthread_mutex_lock(&s->_mutex);
            if (!s->_used) {
                pthread_mutex_unlock(&s->_mutex);
                p->RemoveFromList();
                delete s;
                ++nremoved;
            } else {
                s->take_sample();
                pthread_mutex_unlock(&s->_mutex);
                ++nsampled;
            }
            p = saved_next;
        }
        bool slept = false;
        int64_t now = base::gettimeofday_us();
        _cumulated_time_us += now - abstime;
        abstime += 1000000L;
        while (abstime > now) {
            ::usleep(abstime - now);
            slept = true;
            now = base::gettimeofday_us();
        }
        if (slept) {
            consecutive_nosleep = 0;
        } else {            
            if (++consecutive_nosleep >= WARN_NOSLEEP_THRESHOLD) {
                consecutive_nosleep = 0;
                LOG(WARNING) << "bvar is busy at sampling for "
                             << WARN_NOSLEEP_THRESHOLD << " seconds!";
            }
        }
    }
}

Sampler::Sampler() : _used(true) {
    pthread_mutex_init(&_mutex, NULL);
}

Sampler::~Sampler() {
    pthread_mutex_destroy(&_mutex);
}

void Sampler::schedule() {
    *base::get_leaky_singleton<SamplerCollector>() << this;
}

void Sampler::destroy() {
    pthread_mutex_lock(&_mutex);
    _used = false;
    pthread_mutex_unlock(&_mutex);
}

}  // namespace detail
}  // namespace bvar