// Copyright (c) 2010-2014 Baidu.com, Inc. All Rights Reserved
// Author: Ge,Jun (gejun@baidu.com)
// Date: 2010-12-04 11:59

#include <gtest/gtest.h>
#include <errno.h>
#include "base/thread_local.h"

namespace {

BAIDU_THREAD_LOCAL int * dummy = NULL;
const size_t NTHREAD = 8;
bool processed[NTHREAD+1];
bool deleted[NTHREAD+1];
bool register_check = false;

struct YellObj {
    static int nc;
    static int nd;
    YellObj() {
        printf("Created\n");
        ++nc;
    }
    ~YellObj() {
        printf("Destroyed\n");
        ++nd;
    }
};
int YellObj::nc = 0;
int YellObj::nd = 0;


void check_global_variable() {
    ASSERT_TRUE(processed[NTHREAD]);
    ASSERT_TRUE(deleted[NTHREAD]);
    ASSERT_EQ(2, YellObj::nc);
    ASSERT_EQ(2, YellObj::nd);
}

class BaiduThreadLocalTest : public ::testing::Test{
protected:
    BaiduThreadLocalTest(){
        if (!register_check) {
            register_check = true;
            atexit(check_global_variable);
        }
    };
    virtual ~BaiduThreadLocalTest(){};
    virtual void SetUp() {
    };
    virtual void TearDown() {
    };
};


BAIDU_THREAD_LOCAL void* x;

void* foo(void* arg) {
    x = arg;
    usleep(10000);
    printf("x=%p\n", x);
    return NULL;
}

TEST_F(BaiduThreadLocalTest, thread_local_keyword) {
    pthread_t th[2];
    pthread_create(&th[0], NULL, foo, (void*)1);
    pthread_create(&th[1], NULL, foo, (void*)2);
    pthread_join(th[0], NULL);
    pthread_join(th[1], NULL);
}

void* yell(void*) {
    YellObj* p = base::get_thread_local<YellObj>();
    EXPECT_TRUE(p);
    EXPECT_EQ(2, YellObj::nc);
    EXPECT_EQ(0, YellObj::nd);
    EXPECT_EQ(p, base::get_thread_local<YellObj>());
    EXPECT_EQ(2, YellObj::nc);
    EXPECT_EQ(0, YellObj::nd);
    return NULL;
}

TEST_F(BaiduThreadLocalTest, get_thread_local) {
    YellObj::nc = 0;
    YellObj::nd = 0;
    YellObj* p = base::get_thread_local<YellObj>();
    ASSERT_TRUE(p);
    ASSERT_EQ(1, YellObj::nc);
    ASSERT_EQ(0, YellObj::nd);
    ASSERT_EQ(p, base::get_thread_local<YellObj>());
    ASSERT_EQ(1, YellObj::nc);
    ASSERT_EQ(0, YellObj::nd);
    pthread_t th;
    ASSERT_EQ(0, pthread_create(&th, NULL, yell, NULL));
    pthread_join(th, NULL);
    EXPECT_EQ(2, YellObj::nc);
    EXPECT_EQ(1, YellObj::nd);
}

void delete_dummy(void* arg) {
    *((bool*)arg) = true;
    if (dummy) {
        printf("dummy(%p)=%d\n", dummy, *dummy);
        delete dummy;
        dummy = NULL;
    } else {
        printf("dummy is NULL\n");
    }
}

void* proc_dummy(void* arg) {
    bool *p = (bool*)arg;
    *p = true;
    dummy = new int(p - processed);
    base::thread_atexit(delete_dummy, deleted + (p - processed));
    return NULL;
}

TEST_F(BaiduThreadLocalTest, sanity) {
    errno = 0;
    ASSERT_EQ(-1, base::thread_atexit(NULL));
    ASSERT_EQ(EINVAL, errno);

    processed[NTHREAD] = false;
    deleted[NTHREAD] = false;
    proc_dummy(&processed[NTHREAD]);
    
    pthread_t th[NTHREAD];
    for (size_t i = 0; i < NTHREAD; ++i) {
        processed[i] = false;
        deleted[i] = false;
        ASSERT_EQ(0, pthread_create(&th[i], NULL, proc_dummy, processed + i));
    }
    for (size_t i = 0; i < NTHREAD; ++i) {
        ASSERT_EQ(0, pthread_join(th[i], NULL));
        ASSERT_TRUE(processed[i]);
        ASSERT_TRUE(deleted[i]);
    }
}

std::stringstream oss;

void fun1() {
    oss << "fun1" << std::endl;
}

void fun2() {
    oss << "fun2" << std::endl;
}

void fun3(void* arg) {
    oss << "fun3(" << arg << ")" << std::endl;
}

void fun4(void* arg) {
    oss << "fun4(" << arg << ")" << std::endl;
}

void check_result() {
    ASSERT_EQ("fun4(0)\nfun3(0x2)\nfun2\n", oss.str());
}

TEST_F(BaiduThreadLocalTest, call_order_and_cancel) {
    base::thread_atexit_cancel(NULL);
    base::thread_atexit_cancel(NULL, NULL);

    ASSERT_EQ(0, base::thread_atexit(check_result));

    ASSERT_EQ(0, base::thread_atexit(fun1));
    ASSERT_EQ(0, base::thread_atexit(fun1));
    ASSERT_EQ(0, base::thread_atexit(fun2));
    ASSERT_EQ(0, base::thread_atexit(fun3, (void*)1));
    ASSERT_EQ(0, base::thread_atexit(fun3, (void*)1));
    ASSERT_EQ(0, base::thread_atexit(fun3, (void*)2));
    ASSERT_EQ(0, base::thread_atexit(fun4, NULL));

    base::thread_atexit_cancel(NULL);
    base::thread_atexit_cancel(NULL, NULL);
    base::thread_atexit_cancel(fun1);
    base::thread_atexit_cancel(fun3, NULL);
    base::thread_atexit_cancel(fun3, (void*)1);
}

}