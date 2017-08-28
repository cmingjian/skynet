#ifndef SKYNET_IMP_H
#define SKYNET_IMP_H

struct skynet_config {
	int thread;						// 有多少个工作线程
	int harbor;						// 当前节点的harbor（master-slave模式中使用）
	int profile;					// 是否打开性能统计
	const char * daemon;
	const char * module_path;		// cpath配置项指定的C服务的路径
	const char * bootstrap;			// 引导程序，snlua bootstrap即可
	const char * logger;
	const char * logservice;
};

#define THREAD_WORKER 0
#define THREAD_MAIN 1
#define THREAD_SOCKET 2
#define THREAD_TIMER 3
#define THREAD_MONITOR 4

void skynet_start(struct skynet_config * config);

#endif
