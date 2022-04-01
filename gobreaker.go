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
	StateClosed State = iota
	StateHalfOpen
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

func (c *Counts) onRequest() {
	c.Requests++
}

func (c *Counts) onSuccess() {
	c.TotalSuccesses++
	c.ConsecutiveSuccesses++
	c.ConsecutiveFailures = 0
}

func (c *Counts) onFailure() {
	c.TotalFailures++
	c.ConsecutiveFailures++
	c.ConsecutiveSuccesses = 0
}

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

	// MaxRequests is the maximum number of requests allowed to pass through
	// when the CircuitBreaker is half-open.
	// If MaxRequests is 0, the CircuitBreaker allows only 1 request.
	//
	// MaxRequests是CircuitBreaker半开时允许通过的最大请求数。
	// 如果 MaxRequests 为 0，CircuitBreaker 只允许 1 个请求。
	MaxRequests uint32

	// Interval is the cyclic period of the closed state
	// for the CircuitBreaker to clear the internal Counts.
	// If Interval is less than or equal to 0, the CircuitBreaker doesn't clear internal Counts during the closed state.
	//
	// Interval 是 CircuitBreaker 清除内部 Counts 的关闭状态的循环周期。
	// 如果 Interval 小于等于 0，CircuitBreaker 在关闭状态时不会清除内部 Counts。
	Interval time.Duration

	// Timeout is the period of the open state,
	// after which the state of the CircuitBreaker becomes half-open.
	// If Timeout is less than or equal to 0, the timeout value of the CircuitBreaker is set to 60 seconds.
	//
	// Timeout 是打开状态的时间段，之后CircuitBreaker 的状态变为半打开状态。
	// 如果Timeout小于等于0，则CircuitBreaker的超时值设置为60秒。
	Timeout time.Duration

	// ReadyToTrip is called with a copy of Counts whenever a request fails in the closed state.
	// If ReadyToTrip returns true, the CircuitBreaker will be placed into the open state.
	// If ReadyToTrip is nil, default ReadyToTrip is used.
	// Default ReadyToTrip returns true when the number of consecutive failures is more than 5.
	//
	// 每当请求在关闭状态下失败时，都会使用 Counts 的副本调用 ReadyToTrip。
	// 如果 ReadyToTrip 返回 true，CircuitBreaker 将被置于打开状态。
	// 如果 ReadyToTrip 为 nil，则使用默认的 ReadyToTrip。
	// 默认 ReadyToTrip 在连续失败次数超过 5 次时返回 true。
	ReadyToTrip func(counts Counts) bool

	// OnStateChange is called whenever the state of the CircuitBreaker changes.
	//
	// 只要 CircuitBreaker 的状态发生变化，就会调用 OnStateChange。
	OnStateChange func(name string, from State, to State)

	// IsSuccessful is called with the error returned from a request.
	// If IsSuccessful returns true, the error is counted as a success.
	// Otherwise the error is counted as a failure.
	// If IsSuccessful is nil, default IsSuccessful is used, which returns false for all non-nil errors.
	//
	// IsSuccessful 被调用请求返回的错误。
	// 如果 IsSuccessful 返回 true，则错误计为成功。 否则该错误被视为失败。
	// 如果 IsSuccessful 为 nil，则使用默认 IsSuccessful，对于所有非 nil 错误返回 false。
	IsSuccessful func(err error) bool
}

// CircuitBreaker is a state machine to prevent sending requests that are likely to fail.
//
// CircuitBreaker 是一个状态机，用于防止发送可能失败的请求。
type CircuitBreaker struct {
	name string
	// maxRequests 限制half-open状态下最大的请求数，避免海量请求将在恢复过程中的服务再次失败
	maxRequests uint32
	// interval 用于在closed状态下，断路器多久清除一次Counts信息，如果设置为0则在closed状态下不会清除Counts
	interval time.Duration
	// timeout 进入open状态下，多长时间切换到half-open状态，默认60s
	timeout time.Duration
	// readyToTrip 熔断条件，当执行失败后，会根据readyToTrip决定是否进入Open状态
	readyToTrip  func(counts Counts) bool
	isSuccessful func(err error) bool
	// onStateChange 断路器状态变更回调函数
	onStateChange func(name string, from State, to State)

	mutex sync.Mutex
	// state 断路器状态
	state State
	// generation 是一个递增值，相当于当前断路器状态切换的次数
	// 为了避免状态切换后，未完成请求对新状态的统计的影响，如果发现一个请求的generation同当前的generation不同，则不会进行统计计数
	generation uint64
	// counts 统计
	counts Counts
	// expiry 超时过期用于open状态到half-open状态的切换，当超时后，会从open状态切换到half-open状态
	expiry time.Time
}

// TwoStepCircuitBreaker is like CircuitBreaker but instead of surrounding a function
// with the breaker functionality, it only checks whether a request can proceed and
// expects the caller to report the outcome in a separate step using a callback.
type TwoStepCircuitBreaker struct {
	cb *CircuitBreaker
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
func (cb *CircuitBreaker) Execute(req func() (interface{}, error)) (interface{}, error) {
	generation, err := cb.beforeRequest()
	if err != nil {
		return nil, err
	}

	defer func() {
		e := recover()
		if e != nil {
			cb.afterRequest(generation, false)
			panic(e)
		}
	}()

	result, err := req()
	cb.afterRequest(generation, cb.isSuccessful(err))
	return result, err
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

func (cb *CircuitBreaker) beforeRequest() (uint64, error) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, generation := cb.currentState(now)

	if state == StateOpen {
		return generation, ErrOpenState
	} else if state == StateHalfOpen && cb.counts.Requests >= cb.maxRequests {
		return generation, ErrTooManyRequests
	}

	cb.counts.onRequest()
	return generation, nil
}

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

func (cb *CircuitBreaker) currentState(now time.Time) (State, uint64) {
	switch cb.state {
	case StateClosed:
		if !cb.expiry.IsZero() && cb.expiry.Before(now) {
			cb.toNewGeneration(now)
		}
	case StateOpen:
		if cb.expiry.Before(now) {
			cb.setState(StateHalfOpen, now)
		}
	}
	return cb.state, cb.generation
}

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

func (cb *CircuitBreaker) toNewGeneration(now time.Time) {
	cb.generation++
	cb.counts.clear()

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
