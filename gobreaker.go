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
//三种状态：
//Closed
//Open
//HalfOpen
const (
	StateClosed   State = iota //0
	StateHalfOpen              //1
	StateOpen                  //2	熔断器开启
)

/*
 		Closed
         /    \
 Half-Open <--> Open

初始状态是：Closed，指熔断器放行所有请求。
达到一定数量的错误计数，进入Open 状态，指熔断发生，下游出现错误，不能再放行请求。
经过一段Interval时间后，自动进入Half-Open状态，然后开始尝试对成功请求计数。
进入Half-Open后，根据成功/失败计数情况，会自动进入Closed或Open。
*/

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

//范围: Generation周期内
type Counts struct {
	Requests             uint32 //请求次数
	TotalSuccesses       uint32 // 总共成功次数
	TotalFailures        uint32 // 总共失败次数
	ConsecutiveSuccesses uint32 // 连续成功次数
	ConsecutiveFailures  uint32 // 连续失败次数
}

func (c *Counts) onRequest() {
	c.Requests++
}

func (c *Counts) onSuccess() {
	c.TotalSuccesses++
	c.ConsecutiveSuccesses++
	c.ConsecutiveFailures = 0 //连续失败清0
}

func (c *Counts) onFailure() {
	c.TotalFailures++
	c.ConsecutiveFailures++
	c.ConsecutiveSuccesses = 0 //连续成功清0
}

func (c *Counts) clear() {
	c.Requests = 0
	c.TotalSuccesses = 0
	c.TotalFailures = 0
	c.ConsecutiveSuccesses = 0
	c.ConsecutiveFailures = 0
}

// Settings configures CircuitBreaker:
//
// Name is the name of the CircuitBreaker.
//
// MaxRequests is the maximum number of requests allowed to pass through
// when the CircuitBreaker is half-open.
// If MaxRequests is 0, the CircuitBreaker allows only 1 request.
//
// Interval is the cyclic period of the closed state
// for the CircuitBreaker to clear the internal Counts.
// If Interval is less than or equal to 0, the CircuitBreaker doesn't clear internal Counts during the closed state.
//
// Timeout is the period of the open state,
// after which the state of the CircuitBreaker becomes half-open.
// If Timeout is less than or equal to 0, the timeout value of the CircuitBreaker is set to 60 seconds.
//
// ReadyToTrip is called with a copy of Counts whenever a request fails in the closed state.
// If ReadyToTrip returns true, the CircuitBreaker will be placed into the open state.
// If ReadyToTrip is nil, default ReadyToTrip is used.
// Default ReadyToTrip returns true when the number of consecutive failures is more than 5.
//
// OnStateChange is called whenever the state of the CircuitBreaker changes.
//
// IsSuccessful is called with the error returned from the request, if not nil.
// If IsSuccessful returns false, the error is considered a failure, and is counted towards tripping the circuit breaker.
// If IsSuccessful returns true, the error will be returned to the caller without tripping the circuit breaker.
// If IsSuccessful is nil, default IsSuccessful is used, which returns false for all non-nil errors.

//breaker 配置
type Settings struct {
	Name          string                                  //breaker名称
	MaxRequests   uint32                                  // 最大请求数，用于HelfOpen状态
	Interval      time.Duration                           // Close状态时，定期清除counts （的周期）
	Timeout       time.Duration                           // Open状态timeout后，进入HelfOpen
	ReadyToTrip   func(counts Counts) bool                // Closed状态时,当报错时调用它。当连续错误达到一定数量时，进入Open状态
	OnStateChange func(name string, from State, to State) // 状态变化时调用
	IsSuccessful  func(err error) bool
}

// CircuitBreaker is a state machine to prevent sending requests that are likely to fail.
type CircuitBreaker struct {
	name          string
	maxRequests   uint32
	interval      time.Duration
	timeout       time.Duration
	readyToTrip   func(counts Counts) bool
	isSuccessful  func(err error) bool
	onStateChange func(name string, from State, to State)

	mutex      sync.Mutex
	state      State  //熔断器的当前状态，初始化为0（关闭状态）
	generation uint64 //当前的代数，从0开始
	counts     Counts
	expiry     time.Time
}

// TwoStepCircuitBreaker is like CircuitBreaker but instead of surrounding a function
// with the breaker functionality, it only checks whether a request can proceed and
// expects the caller to report the outcome in a separate step using a callback.
type TwoStepCircuitBreaker struct {
	cb *CircuitBreaker
}

// NewCircuitBreaker returns a new CircuitBreaker configured with the given Settings.
//初始化对象
func NewCircuitBreaker(st Settings) *CircuitBreaker {
	cb := new(CircuitBreaker)

	cb.name = st.Name
	cb.onStateChange = st.OnStateChange //onStateChange为用户传入的自定义函数

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

	//初始化cb的expiry时间
	cb.toNewGeneration(time.Now())

	return cb
}

// NewTwoStepCircuitBreaker returns a new TwoStepCircuitBreaker configured with the given Settings.
func NewTwoStepCircuitBreaker(st Settings) *TwoStepCircuitBreaker {
	return &TwoStepCircuitBreaker{
		cb: NewCircuitBreaker(st),
	}
}

const defaultInterval = time.Duration(0) * time.Second //0S
const defaultTimeout = time.Duration(60) * time.Second //60S

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
//获取当前的熔断器状态，需要原子操作
func (cb *CircuitBreaker) State() State {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	//获取当前的状态
	state, _ := cb.currentState(now)
	return state
}

// Counts returns internal counters
// 获取当前cb的统计结构
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
//核心执行函数Execute： 该函数分为三步 beforeRequest、 执行请求、 afterRequest
func (cb *CircuitBreaker) Execute(req func() (interface{}, error)) (interface{}, error) {
	generation, err := cb.beforeRequest()
	if err != nil {
		return nil, err
	}

	defer func() {
		e := recover()
		if e != nil {
			cb.afterRequest(generation, false)
			panic(e) //if panic，继续panic给上层调用者去recover，有趣
		}
	}()

	//执行真正的用户调用
	result, err := req()

	//调用后更新熔断器状态
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

/*
beforeRequest函数的核心功能：判断是否放行请求，计数或达到切换新条件刚切换。
1. 判断是否Closed，如是，放行所有请求。
	-- 并且判断时间是否达到Interval周期，从而清空计数，进入新周期，调用toNewGeneration()
2. 如果是Open状态，返回ErrOpenState，不放行所有请求。
	-- 同样判断周期时间，到达则 同样调用 toNewGeneration()，清空计数
3. 如果是half-open状态，则判断是否已放行MaxRequests个请求，如未达到刚放行；否则返回:ErrTooManyRequests。
4. 此函数一旦放行请求，就会对请求计数加1（conut.onRequest())，请求后到另一个关键函数 : afterRequest()。
*/
func (cb *CircuitBreaker) beforeRequest() (uint64, error) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	//获取当前熔断器的状态和generation
	state, generation := cb.currentState(now)

	if state == StateOpen {
		//若打开，禁止请求
		return generation, ErrOpenState
	} else if state == StateHalfOpen && cb.counts.Requests >= cb.maxRequests {
		//half-open状态 && 请求超量，也拒绝请求
		return generation, ErrTooManyRequests
	}

	//其他情况，放行请求，走到afterRequest逻辑
	cb.counts.onRequest()
	return generation, nil
}

/*
函数核心内容很简单，就对成功/失败进行计数，达到条件则切换状态。
与beforeRequest一样，会调用公共函数 currentState(now)
currentState(now) 先判断是否进入一个先的计数时间周期(Interval), 是则重置计数，改变熔断器状态，并返回新一代。
如果request耗时大于Interval, 几本每次都会进入新的计数周期，熔断器就没什么意义了
*/
func (cb *CircuitBreaker) afterRequest(before uint64, success bool) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, generation := cb.currentState(now)
	if generation != before {
		//说明，在currentState已经更新了代数，直接返回吧
		return
	}

	//否则，说明还在同一代中，根据err（是否为nil，这里比较简单）更新计数
	if success {
		//更新succ
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
		//在half-open状态下，如果（当前这代counts中）连续succ的数目超过maxRequests，那么则重置当前熔断器的状态为closed（关闭）
		cb.counts.onSuccess()
		if cb.counts.ConsecutiveSuccesses >= cb.maxRequests {
			cb.setState(StateClosed, now)
		}
		//这里不可能出现stateOpen状态
	}
}

// 调用失败情况下的处理
func (cb *CircuitBreaker) onFailure(state State, now time.Time) {
	switch state {
	case StateClosed:
		cb.counts.onFailure() //失败计数++
		if cb.readyToTrip(cb.counts) {
			//调用触发熔断器由关闭=>打开的判断方法（可由用户传入，默认方法defaultReadyToTrip是连续的错误次数>5）
			//设置熔断器为打开状态
			cb.setState(StateOpen, now)
		}
	case StateHalfOpen:
		//在half-open情况下，如果仍然调用失败，那么继续把熔断器设置为打开状态
		cb.setState(StateOpen, now)
	}
}

//currentState: 获取当前状态
//1、当Closed时且expiry过期，调用toNewGeneration生成新的generation
//2、当Open时且expiry过期，设为halfOpen
func (cb *CircuitBreaker) currentState(now time.Time) (State, uint64) {
	switch cb.state {
	//熔断器关闭时
	case StateClosed:
		if !cb.expiry.IsZero() /*cb.expiry非0值*/ && cb.expiry.Before(now) /*cb.expiry比now早，说明cb.expiry过期*/ {
			//需要重新生成一个周期
			cb.toNewGeneration(now)
		}
		//否则不需要
	case StateOpen:
		//熔断器打开时
		if cb.expiry.Before(now) {
			//如果打开时，cb.expiry过期，那么熔断器需要进入half-open状态
			//注意：在此来完成从熔断器打开=>熔断器半打开的触发逻辑！！！！！
			cb.setState(StateHalfOpen, now)
		}
	}
	return cb.state, cb.generation
}

//设置当前熔断器状态
func (cb *CircuitBreaker) setState(state State, now time.Time) {
	if cb.state == state {
		//无需设置
		return
	}

	prev := cb.state
	cb.state = state
	//每当设置新状态时，需要重置当前的generation
	cb.toNewGeneration(now)

	//如果用户设置了状态变迁回调，那么就调用
	if cb.onStateChange != nil {
		cb.onStateChange(cb.name, prev, state)
	}
}

//toNewGeneration: 生成新的generation。 主要是清空counts和设置expiry（过期时间）
//1. 当状态为Closed时expiry为Closed的过期时间（当前时间 + interval）
//2. 当状态为Open时expiry为Open的过期时间（当前时间 + timeout）

func (cb *CircuitBreaker) toNewGeneration(now time.Time) {
	cb.generation++
	//清空单个周期内的计数结构
	cb.counts.clear()

	var zero time.Time
	switch cb.state {
	//当熔断器在CLOSE状态下
	case StateClosed:
		if cb.interval == 0 {
			//defaultInterval
			cb.expiry = zero
		} else {
			//
			cb.expiry = now.Add(cb.interval)
		}
	case StateOpen:
		cb.expiry = now.Add(cb.timeout)
	default: // StateHalfOpen
		cb.expiry = zero
	}
}
