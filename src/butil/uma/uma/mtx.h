/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Author: Xu Yifeng
 */

#ifndef UMA_MUTEX_H
#define	UMA_MUTEX_H

#include <pthread.h>
#include "cdefs.h"

#define	MTX_DEF		0x00000000
#define MTX_SPIN	0x00000001
#define MTX_RECURSE	0x00000004
#define	MTX_NOWITNESS	0x00000008
#define MTX_NOPROFILE   0x00000020
#define	MTX_NEW		0x00000040

#define	MTX_QUIET	0x080
#define	MTX_DUPOK	0x100

struct uma_mtx {
	pthread_mutex_t	mtx_lock;
	pthread_t       mtx_owner;
};

#define MA_OWNED	0
#define MA_NOTOWNED	1
#define MA_RECURSED	3
#define MA_NOTRECURSED	4

void uma_mtx_init(struct uma_mtx *m, const char *name, const char *type, int opts);
void uma_mtx_destroy(struct uma_mtx *m);
int  uma_mtx_trylock(struct uma_mtx *m);
void uma_mtx_lock(struct uma_mtx *m);

#define uma_mtx_lock_flags(m, f) uma_mtx_lock(m)

void uma_mtx_unlock(struct uma_mtx *m);
void uma_mtx_assert(struct uma_mtx *m, int); 
void uma_mtx_sleep(pthread_cond_t *cond, struct uma_mtx *m);
#endif /* !UMA_MUTEX_H */
