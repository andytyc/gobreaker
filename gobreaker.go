// Package gobreaker implements the Circuit Breaker pattern.
// See https://msdn.microsoft.com/en-us/library/dn589784.aspx.
package gobreaker

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// State is a type that represents a state of CircuitBreaker.
type State int

// These constants are states of CircuitBreaker.
const (
	// 关 允许所有请求
	StateClosed State = iota
	// 半开 允许部分请求, 但有请求并发量上限约束
	StateHalfOpen
	// 开 拒绝所有请求
	StateOpen
)

var (
	// ErrTooManyRequests is returned when the CB state is half open and the requests count is over the cb maxRequests
	ErrTooManyRequests = errors.New("too many requests")
	// ErrOpenState is returned when the CB state is open
	ErrOpenState = errors.New("circuit breaker is open")
)

// String implements stringer interface.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half-open"
	case StateOpen:
		return "open"
	default:
		return fmt.Sprintf("unknown state: %d", s)
	}
}

// Counts holds the numbers of requests and their successes/failures.
// CircuitBreaker clears the internal Counts either
// on the change of the state or at the closed-state intervals.
// Counts ignores the results of the requests sent before clearing.
//
// Counts就是一个计数器，记录当前请求成功和失败的数量
//
// Counts 保存请求的数量及其成功/失败。
// CircuitBreaker 在状态变化或关闭状态间隔时清除内部计数。
// Counts 忽略清除前发送的请求的结果。
type Counts struct {
	// 请求数
	Requests uint32
	// 成功
	TotalSuccesses uint32
	// 失败
	TotalFailures uint32
	// 连续成功
	ConsecutiveSuccesses uint32
	// 连续失败
	ConsecutiveFailures uint32
}

// onRequest 请求允许尝试处理时调用
func (c *Counts) onRequest() {
	c.Requests++
}

// onSuccess 请求尝试处理成功时调用
func (c *Counts) onSuccess() {
	c.TotalSuccesses++
	c.ConsecutiveSuccesses++
	c.ConsecutiveFailures = 0
}

// onFailure 请求尝试处理失败时调用
func (c *Counts) onFailure() {
	c.TotalFailures++
	c.ConsecutiveFailures++
	c.ConsecutiveSuccesses = 0
}

// clear 将计数器重置清空
func (c *Counts) clear() {
	c.Requests = 0
	c.TotalSuccesses = 0
	c.TotalFailures = 0
	c.ConsecutiveSuccesses = 0
	c.ConsecutiveFailures = 0
}

// Settings configures CircuitBreaker:
type Settings struct {
	// Name is the name of the CircuitBreaker.
	Name string

	// MaxRequests halfopen: 允许通过的最大请求数，默认：1个请求。
	MaxRequests uint32

	// Interval is the cyclic period of the closed state
	// for the CircuitBreaker to clear the internal Counts.
	// If Interval is less than or equal to 0, the CircuitBreaker doesn't clear internal Counts during the closed state.
	//
	// Interval 间隔 是 CircuitBreaker 清除内部 Counts 的关闭状态的循环周期。
	// 如果 Interval 小于等于 0，CircuitBreaker 在关闭状态时不会清除内部 Counts。
	Interval time.Duration

	// Timeout is the period of the open state,
	// after which the state of the CircuitBreaker becomes half-open.
	// If Timeout is less than or equal to 0, the timeout value of the CircuitBreaker is set to 60 seconds.
	//
	// Timeout "open => halfopen" 是打开状态的时间段，之后CircuitBreaker 的状态变为半打开状态。
	// 如果Timeout小于等于0，则CircuitBreaker的超时值设置为60秒。
	Timeout time.Duration

	// ReadyToTrip is called with a copy of Counts whenever a request fails in the closed state.
	// If ReadyToTrip returns true, the CircuitBreaker will be placed into the open state.
	// If ReadyToTrip is nil, default ReadyToTrip is used.
	// Default ReadyToTrip returns true when the number of consecutive failures is more than 5.
	//
	// ready to trip 准备旅行, "closed => open"
	//
	// 每当请求在关闭状态下失败时，都会使用 Counts 的副本调用 ReadyToTrip。
	//
	// 如果 ReadyToTrip 返回 true，CircuitBreaker 将被置于打开状态。
	//
	// 如果未配置 Setting.ReadyToTrip == nil，则使用默认的 ReadyToTrip (有默认封装逻辑)。
	// 即，默认 ReadyToTrip: 在连续失败次数超过 5 次时返回 true 然后 CircuitBreaker 将被置于打开状态。 。
	ReadyToTrip func(counts Counts) bool

	// OnStateChange is called whenever the state of the CircuitBreaker changes.
	//
	// OnStateChange 触发函数 只要 CircuitBreaker 的状态发生变化，就会调用 OnStateChange。
	OnStateChange func(name string, from State, to State)

	// IsSuccessful is called with the error returned from a request.
	// If IsSuccessful returns true, the error is counted as a success.
	// Otherwise the error is counted as a failure.
	// If IsSuccessful is nil, default IsSuccessful is used, which returns false for all non-nil errors.
	//
	// IsSuccessful 传入请求返回的error, 也就是对请求成功还是失败的一个函数判断，逻辑可以自定义，不定义则采用默认函数
	//
	// 如果 IsSuccessful 返回 true，则认为是请求成功。否则被视为失败。
	//
	// 如果未配置 Setting.IsSuccessful == nil，则使用默认 IsSuccessful，它对于所有非 nil 错误error返回 false，都认为请求失败。
	IsSuccessful func(err error) bool
}

// CircuitBreaker is a state machine to prevent sending requests that are likely to fail.
//
// CircuitBreaker 是一个状态机，用于防止发送可能失败的请求。
type CircuitBreaker struct {
	name string

	// maxRequests 半开状态下的最大的请求数
	//
	// half-open: 最大的请求数，避免海量请求将在恢复过程中的服务再次失败
	//
	// halfopen: 当 "连续请求成功 >= maxRequests", 改变状态 => closed
	maxRequests uint32

	// interval 间隔/周期，默认0
	//
	// closed: 为0, 则不进行清除Counts计数器
	//
	// closed: 这段时间都是请求成功的，超时后，重新计数，即：清除Counts计数器
	interval time.Duration

	// timeout 超时时间, 默认60s
	//
	// open 超时后，改变状态为 => halfopen(开始接收部分请求)
	timeout time.Duration

	// closed: 熔断条件，若返回true，表示满足条件，进行熔断，改变状态 => open
	readyToTrip func(counts Counts) bool

	// isSuccessful 判断请求返回的error: 是否认定为请求成功或请求失败
	isSuccessful func(err error) bool

	// onStateChange 触发函数 断路器状态变更触发回调函数
	onStateChange func(name string, from State, to State)

	mutex sync.Mutex

	// state 断路器状态
	state State

	// generation 是一个递增值，相当于当前断路器状态切换的次数
	//
	// 为了避免状态切换后，未完成请求对新状态的统计的影响(这里意思比如：请求A开始处理时，是halfopen, 处理完毕后，变成了open)
	// 如果发现一个请求的generation同当前的generation不同，则不会进行统计计数(不算数，忽略这次统计)
	generation uint64

	// counts 统计
	counts Counts

	// expiry 记录不同状态下的超时时间，状态发生变化的超时时间
	//
	// closed: 超时时间是interval, 默认:interval==0,即:不重制计数器Counts，否则，超时后，重置计数器
	//
	// open: 超时时间是timeout, 默认:timeout==60s, 超时后 => halfopen 开始允许部分请求
	//
	// halfopen: 超时时间是0, 即:没意义，半开用不到超时时间
	expiry time.Time
}

// NewCircuitBreaker returns a new CircuitBreaker configured with the given Settings.
func NewCircuitBreaker(st Settings) *CircuitBreaker {
	cb := new(CircuitBreaker)

	cb.name = st.Name
	cb.onStateChange = st.OnStateChange

	if st.MaxRequests == 0 {
		cb.maxRequests = 1
	} else {
		cb.maxRequests = st.MaxRequests
	}

	if st.Interval <= 0 {
		cb.interval = defaultInterval
	} else {
		cb.interval = st.Interval
	}

	if st.Timeout <= 0 {
		cb.timeout = defaultTimeout
	} else {
		cb.timeout = st.Timeout
	}

	if st.ReadyToTrip == nil {
		cb.readyToTrip = defaultReadyToTrip
	} else {
		cb.readyToTrip = st.ReadyToTrip
	}

	if st.IsSuccessful == nil {
		cb.isSuccessful = defaultIsSuccessful
	} else {
		cb.isSuccessful = st.IsSuccessful
	}

	cb.toNewGeneration(time.Now())

	return cb
}

// TwoStepCircuitBreaker is like CircuitBreaker but instead of surrounding a function
// with the breaker functionality, it only checks whether a request can proceed and
// expects the caller to report the outcome in a separate step using a callback.
//
// TwoStepCircuitBreaker 与 CircuitBreaker 类似，但它不是用断路器功能包围一个函数，它只检查请求是否可以继续，并期望调用者使用回调在单独的步骤中报告结果。
type TwoStepCircuitBreaker struct {
	cb *CircuitBreaker
}

// NewTwoStepCircuitBreaker returns a new TwoStepCircuitBreaker configured with the given Settings.
func NewTwoStepCircuitBreaker(st Settings) *TwoStepCircuitBreaker {
	return &TwoStepCircuitBreaker{
		cb: NewCircuitBreaker(st),
	}
}

const defaultInterval = time.Duration(0) * time.Second
const defaultTimeout = time.Duration(60) * time.Second

func defaultReadyToTrip(counts Counts) bool {
	return counts.ConsecutiveFailures > 5
}

func defaultIsSuccessful(err error) bool {
	return err == nil
}

// Name returns the name of the TwoStepCircuitBreaker.
func (tscb *TwoStepCircuitBreaker) Name() string {
	return tscb.cb.Name()
}

// State returns the current state of the TwoStepCircuitBreaker.
func (tscb *TwoStepCircuitBreaker) State() State {
	return tscb.cb.State()
}

// Counts returns internal counters
func (tscb *TwoStepCircuitBreaker) Counts() Counts {
	return tscb.cb.Counts()
}

// Allow checks if a new request can proceed. It returns a callback that should be used to
// register the success or failure in a separate step. If the circuit breaker doesn't allow
// requests, it returns an error.
func (tscb *TwoStepCircuitBreaker) Allow() (done func(success bool), err error) {
	generation, err := tscb.cb.beforeRequest()
	if err != nil {
		return nil, err
	}

	return func(success bool) {
		tscb.cb.afterRequest(generation, success)
	}, nil
}

// Name returns the name of the CircuitBreaker.
func (cb *CircuitBreaker) Name() string {
	return cb.name
}

// State returns the current state of the CircuitBreaker.
func (cb *CircuitBreaker) State() State {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, _ := cb.currentState(now)
	return state
}

// Counts returns internal counters
func (cb *CircuitBreaker) Counts() Counts {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	return cb.counts
}

// Execute runs the given request if the CircuitBreaker accepts it.
// Execute returns an error instantly if the CircuitBreaker rejects the request.
// Otherwise, Execute returns the result of the request.
// If a panic occurs in the request, the CircuitBreaker handles it as an error
// and causes the same panic again.
//
// 如果 CircuitBreaker 接受，Execute 运行给定的请求。
// 如果 CircuitBreaker 拒绝请求，Execute 会立即返回错误。
// 否则，Execute 返回请求的结果。
// 如果请求中发生恐慌，CircuitBreaker 将其作为错误处理并再次导致相同的恐慌。
func (cb *CircuitBreaker) Execute(req func() (interface{}, error)) (interface{}, error) {
	// 请求是否允许
	generation, err := cb.beforeRequest()
	if err != nil {
		return nil, err
	}

	// 捕获panic，避免应用函数错误造成断路器panic
	defer func() {
		e := recover()
		if e != nil {
			cb.afterRequest(generation, false)
			panic(e)
		}
	}()

	// 处理请求req
	result, err := req()

	// 处理请求完毕, 传递generation并更新状态统计
	cb.afterRequest(generation, cb.isSuccessful(err))

	// 返回请求结果
	return result, err
}

// beforeRequest 请求前钩子
// 处理请求前，会根据当前状态，来返回当前的generation和err(如果位于open和half-open(>= max request)则不为nil)
func (cb *CircuitBreaker) beforeRequest() (uint64, error) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, generation := cb.currentState(now)

	// 拒绝请求
	if state == StateOpen {
		return generation, ErrOpenState
	} else if state == StateHalfOpen && cb.counts.Requests >= cb.maxRequests {
		return generation, ErrTooManyRequests
	}

	// 允许请求
	cb.counts.onRequest()

	return generation, nil
}

// afterRequest 请求后钩子
// 处理请求完毕, 传递generation并更新状态统计
func (cb *CircuitBreaker) afterRequest(before uint64, success bool) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, generation := cb.currentState(now)
	if generation != before {
		return
	}

	if success {
		cb.onSuccess(state, now)
	} else {
		cb.onFailure(state, now)
	}
}

// 处理请求完毕, 成功
func (cb *CircuitBreaker) onSuccess(state State, now time.Time) {
	switch state {
	case StateClosed:
		cb.counts.onSuccess()
	case StateHalfOpen:
		cb.counts.onSuccess()
		if cb.counts.ConsecutiveSuccesses >= cb.maxRequests {
			cb.setState(StateClosed, now)
		}
	}
}

// 处理请求完毕, 失败
func (cb *CircuitBreaker) onFailure(state State, now time.Time) {
	switch state {
	case StateClosed:
		cb.counts.onFailure()
		if cb.readyToTrip(cb.counts) {
			cb.setState(StateOpen, now)
		}
	case StateHalfOpen:
		cb.setState(StateOpen, now)
	}
}

// currentState 获取当前的状态
// 注意: 这里类似一个"用户"手动触发，来触发：看看是否需要更新必要的操作，如：重制统计状态，open(不允许请求) -> halfopen(允许部分请求)
func (cb *CircuitBreaker) currentState(now time.Time) (State, uint64) {
	switch cb.state {
	case StateClosed:
		// closed: 当前已经超时, 说明目前请求比较少，时间间隔长，重新统计状态, 状态还是:closed
		if !cb.expiry.IsZero() && cb.expiry.Before(now) {
			cb.toNewGeneration(now)
		}
	case StateOpen:
		// open: 当前已经超时, 改变状态 -> halfopen:允许部分请求进来
		if cb.expiry.Before(now) {
			cb.setState(StateHalfOpen, now)
		}
	}
	// 其他state类型，如:halfopen 无需处理，保持
	return cb.state, cb.generation
}

// setState 改变状态, 并做必要的事: 新建一个统计状态周期, 触发回调函数
func (cb *CircuitBreaker) setState(state State, now time.Time) {
	if cb.state == state {
		return
	}

	prev := cb.state
	cb.state = state

	cb.toNewGeneration(now)

	if cb.onStateChange != nil {
		cb.onStateChange(cb.name, prev, state)
	}
}

// toNewGeneration 新建一个统计状态周期: 递增generation, 清除计数器, 设置超时时间
func (cb *CircuitBreaker) toNewGeneration(now time.Time) {
	// 递增generation, 清除计数器
	cb.generation++
	cb.counts.clear()

	// 设置超时时间
	var zero time.Time
	switch cb.state {
	case StateClosed:
		if cb.interval == 0 {
			cb.expiry = zero
		} else {
			cb.expiry = now.Add(cb.interval)
		}
	case StateOpen:
		cb.expiry = now.Add(cb.timeout)
	default: // StateHalfOpen
		cb.expiry = zero
	}
}
