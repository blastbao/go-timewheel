package timewheel

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	typeTimer taskType = iota
	typeTicker

	modeIsCircle  = true
	modeNotCircle = false

	modeIsAsync  = true
	modeNotAsync = false
)

type taskType int64
type taskID int64

type Task struct {
	delay    time.Duration
	id       taskID
	round    int
	callback func()

	async  bool
	stop   bool
	circle bool
	// circleNum int
}

// for sync.Pool
func (t *Task) Reset() {
	t.round = 0
	t.callback = nil

	t.async = false
	t.stop = false
	t.circle = false
}

type optionCall func(*TimeWheel) error

func TickSafeMode() optionCall {
	return func(o *TimeWheel) error {
		o.tickQueue = make(chan time.Time, 10)
		return nil
	}
}

func SetSyncPool(state bool) optionCall {
	return func(o *TimeWheel) error {
		o.syncPool = state
		return nil
	}
}

type TimeWheel struct {
	randomID int64

	tick      time.Duration
	ticker    *time.Ticker
	tickQueue chan time.Time

	bucketsNum    int
	buckets       []map[taskID]*Task // key: added item, value: *Task
	bucketIndexes map[taskID]int     // key: added item, value: bucket position

	currentIndex int

	onceStart sync.Once

	addC    chan *Task
	removeC chan *Task
	stopC   chan struct{}

	exited   bool
	syncPool bool
}






// NewTimeWheel create new time wheel
func NewTimeWheel(tick time.Duration, bucketsNum int, options ...optionCall) (*TimeWheel, error) {

	// parameter validity check

	if tick.Seconds() < 0.1 {
		return nil, errors.New("invalid params, must tick >= 100 ms")
	}

	if bucketsNum <= 0 {
		return nil, errors.New("invalid params, must bucketsNum > 0")
	}


	tw := &TimeWheel{

		// tick
		tick:      tick,
		tickQueue: make(chan time.Time, 10),


		// store
		bucketsNum:    bucketsNum,
		bucketIndexes: make(map[taskID]int, 1024*100),
		buckets:       make([]map[taskID]*Task, bucketsNum),
		currentIndex:  0,


		// signal
		addC:    make(chan *Task, 1024*5),
		removeC: make(chan *Task, 1024*2),
		stopC:   make(chan struct{}),

	}

	// initial each bucket
	for i := 0; i < bucketsNum; i++ {
		tw.buckets[i] = make(map[taskID]*Task, 16)
	}

	// set options
	for _, op := range options {
		op(tw)
	}

	return tw, nil
}

// Start start the time wheel
func (tw *TimeWheel) Start() {
	// onlye once start
	tw.onceStart.Do(
		func() {
			tw.ticker = time.NewTicker(tw.tick)
			go tw.schduler()
			go tw.tickGenerator()
		},
	)
}

func (tw *TimeWheel) tickGenerator() {
	if tw.tickQueue != nil {
		return
	}

	for !tw.exited {
		select {
		case <-tw.ticker.C:
			select {
			case tw.tickQueue <- time.Now():
			default:
				panic("raise long time blocking")
			}
		}
	}
}



// main loop for add/remove timer and handle tick event
func (tw *TimeWheel) schduler() {
	queue := tw.ticker.C
	if tw.tickQueue == nil {
		queue = tw.tickQueue
	}

	for {
		select {
		case <-queue:
			tw.handleTick()
		case task := <-tw.addC:
			tw.put(task)
		case key := <-tw.removeC:
			tw.remove(key)
		case <-tw.stopC:
			tw.exited = true
			tw.ticker.Stop()
			return
		}
	}
}

// Stop stop the time wheel
func (tw *TimeWheel) Stop() {
	tw.stopC <- struct{}{}
}


// 回收 task 对象
func (tw *TimeWheel) collectTask(task *Task) {

	// 删除正向关系: bucketIndexes[task.id] => bucket index
	delete(tw.bucketIndexes, task.id)
	// 删除反向关系: buckets[index][task.id] => task
	delete(tw.buckets[tw.currentIndex], task.id)
	// 回收 task 对象，以便再利用
	if tw.syncPool {
		defaultTaskPool.put(task)
	}
}

func (tw *TimeWheel) handleTick() {

	// 取出当前 tick 对应的 bucket
	bucket := tw.buckets[tw.currentIndex]

	// 遍历当前 bucket 内的任务
	for k, task := range bucket {

		// 对已结束任务进行清理和回收，以便再利用
		if task.stop {
			tw.collectTask(task)
			continue
		}

		// 如果 round 非 0 意味着当前轮次不应执行，进行 round-- 后 continue
		if bucket[k].round > 0 {
			bucket[k].round--
			continue
		}

		// 如果是异步执行任务，就通过 go 后台执行，否则同步调用等待处理完毕。
		if task.async {

			go task.callback()

		} else {
			// ???
			// 这里有个疑问，因为 index 的增加是定时器驱动的，如果同步调用发生阻塞且耗时超过一个 tick ，
			// 那么会导致整体的延迟累积，整体的定时都会滞后。

			// TODO: optimize gopool
			task.callback()
		}

		// circle
		if task.circle == true {
			// ???
			// 这里是不是会有很多重复的无效调用，或者应该放到 `tw.collectTask(task)` 后面执行？
			tw.putCircle(task, modeIsCircle)
			continue
		}

		// gc
		tw.collectTask(task)
	}


	// tw.currentIndex = (tw.currentIndex + 1 ) % len(tw.bucketsNum)
	if tw.currentIndex == tw.bucketsNum-1 {
		tw.currentIndex = 0
		return
	}
	tw.currentIndex++
}


// Add add an task
func (tw *TimeWheel) Add(delay time.Duration, callback func()) *Task {
	return tw.addAny(delay, callback, modeNotCircle, modeIsAsync)
}


// AddCron add interval task
func (tw *TimeWheel) AddCron(delay time.Duration, callback func()) *Task {
	return tw.addAny(delay, callback, modeIsCircle, modeIsAsync)
}




//参数说明:
// delay: 定时间隔
// callback: 定时回调函数
// circle: cron 模式，重复执行定时任务
// async: 同/异步 模式，如果是同步模式则同步等待回调函数执行结束，如果是异步模式则直接 go callback()
func (tw *TimeWheel) addAny(delay time.Duration, callback func(), circle, async bool) *Task {

	// 检查定时时间
	if delay <= 0 {
		delay = tw.tick
	}

	// 生成任务 id
	id := tw.genUniqueID()

	// 创建任务对象
	var task *Task
	if tw.syncPool {
		task = defaultTaskPool.get()
	} else {
		task = new(Task)
	}

	// 填充任务参数
	task.delay = delay
	task.id = id
	task.callback = callback
	task.circle = circle
	task.async = async // refer to src/runtime/time.go

	// 新增定时任务
	tw.addC <- task
	return task
}


func (tw *TimeWheel) put(task *Task) {
	tw.store(task, false)
}


func (tw *TimeWheel) putCircle(task *Task, circleMode bool) {
	tw.store(task, circleMode)
}

func (tw *TimeWheel) store(task *Task, circleMode bool) {

	round := tw.calculateRound(task.delay)
	index := tw.calculateIndex(task.delay)

	if round > 0 && circleMode {
		task.round = round - 1
	} else {
		task.round = round
	}

	// 保存 `task` 和 `bucket` 的映射关系
	//  (1) 正向关系: task.id => bucket index
	tw.bucketIndexes[task.id] = index
	//  (2) 反向关系: bucket[index][task.id] => task
	tw.buckets[index][task.id] = task

}

func (tw *TimeWheel) calculateRound(delay time.Duration) (round int) {
	delaySeconds := delay.Seconds()
	tickSeconds := tw.tick.Seconds()
	round = int(delaySeconds / tickSeconds / float64(tw.bucketsNum))
	return
}

func (tw *TimeWheel) calculateIndex(delay time.Duration) (index int) {
	delaySeconds := delay.Seconds()
	tickSeconds := tw.tick.Seconds()
	index = (int(float64(tw.currentIndex) + delaySeconds/tickSeconds)) % tw.bucketsNum

	// 注意，根据 handleTick 函数可知 tw.currentIndex = (tw.currentIndex + 1 ) % len(tw.bucketsNum)，
	// 所以 tw.currentIndex 数值不可能超过 tw.bucketsNum，因此上面 index 的计算公式可以简化为更易于理解的方式：
	// index = tw.currentIndex + (delaySeconds/tickSeconds) % tw.bucketsNum

	return
}



func (tw *TimeWheel) Remove(task *Task) error {
	tw.removeC <- task
	return nil
}



func (tw *TimeWheel) remove(task *Task) {
	tw.collectTask(task)
}



func (tw *TimeWheel) NewTimer(delay time.Duration) *Timer {


	queue := make(chan bool, 1) // buf = 1, refer to src/time/sleep.go



	task := tw.addAny(
		delay,
		func() {
			notfiyChannel(queue)
		},
		modeNotCircle,
		modeNotAsync,
	)


	// init timer
	ctx, cancel := context.WithCancel(context.Background())
	timer := &Timer{
		tw:     tw,
		C:      queue, // faster
		task:   task,
		Ctx:    ctx,
		cancel: cancel,
	}


	return timer

}



func (tw *TimeWheel) AfterFunc(delay time.Duration, callback func()) *Timer {


	queue := make(chan bool, 1)


	task := tw.addAny(
		delay,
		func() {
			callback()
			notfiyChannel(queue)
		},
		modeNotCircle, modeIsAsync,
	)


	// init timer
	ctx, cancel := context.WithCancel(context.Background())
	timer := &Timer{
		tw:     tw,
		C:      queue, // faster
		task:   task,
		Ctx:    ctx,
		cancel: cancel,
		fn:     callback,
	}

	return timer
}



func (tw *TimeWheel) NewTicker(delay time.Duration) *Ticker {


	queue := make(chan bool, 1)

	task := tw.addAny(
		delay,
		func() {
			notfiyChannel(queue)
		},
		modeIsCircle,
		modeNotAsync,
	)

	// init ticker
	ctx, cancel := context.WithCancel(context.Background())


	ticker := &Ticker{
		task:   task,
		tw:     tw,
		C:      queue,
		Ctx:    ctx,
		cancel: cancel,
	}


	return ticker
}



func (tw *TimeWheel) After(delay time.Duration) <-chan time.Time {

	queue := make(chan time.Time, 1)

	tw.addAny(
		delay,
		func() {
			queue <- time.Now()
		},
		modeNotCircle,
		modeNotAsync,
	)

	return queue
}



func (tw *TimeWheel) Sleep(delay time.Duration) {

	queue := make(chan bool, 1)

	tw.addAny(
		delay,
		func() {
			queue <- true
		},
		modeNotCircle,
		modeNotAsync,
	)

	<-queue
}



// similar to golang std timer
type Timer struct {
	task *Task
	tw   *TimeWheel
	fn   func() // external custom func
	C    chan bool

	cancel context.CancelFunc
	Ctx    context.Context
}



func (t *Timer) Reset(delay time.Duration) {
	var task *Task
	if t.fn != nil { // use AfterFunc
		task = t.tw.addAny(
			delay,
			func() {
				t.fn()
				notfiyChannel(t.C)
			},
			modeNotCircle,
			modeIsAsync, // must async mode
		)
	} else {
		task = t.tw.addAny(
			delay,
			func() {
				notfiyChannel(t.C)
			},
			modeNotCircle,
			modeNotAsync,
		)
	}

	t.task = task
}



func (t *Timer) Stop() {
	t.task.stop = true
	t.cancel()
	t.tw.Remove(t.task)
}



func (t *Timer) StopFunc(callback func()) {
	t.fn = callback
}



type Ticker struct {
	tw     *TimeWheel
	task   *Task
	cancel context.CancelFunc

	C   chan bool
	Ctx context.Context
}



func (t *Ticker) Stop() {
	t.task.stop = true
	t.cancel()
	t.tw.Remove(t.task)
}

func notfiyChannel(q chan bool) {
	select {
	case q <- true:
	default:
	}
}

func (tw *TimeWheel) genUniqueID() taskID {
	id := atomic.AddInt64(&tw.randomID, 1)
	return taskID(id)
}


