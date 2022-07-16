package main

import (
	"container/list"
	"fmt"
	"net/http"
	"sync"
)

func main() {
	flowControl := NewFlowControl()
	myHandler := MyHandler{
		flowControl: flowControl,
	}
	http.Handle("/", &myHandler)

	http.ListenAndServe(":8080", nil)
}

type MyHandler struct {
	flowControl *FlowControl
}

//func (h *MyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
//	w.Write([]byte("Hello Go"))
//}

func (h *MyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Println("recieve http request")
	job := &Job{
		DoneChan: make(chan struct{}, 1),
		handleJob: func(job *Job) error {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte("Hello World"))
			return nil
		},
	}

	h.flowControl.CommitJob(job)
	fmt.Println("commit job to job queue success")
	job.WaitDone()
}

type FlowControl struct {
	jobQueue *JobQueue
	wm       *WorkerManager
}

func NewFlowControl() *FlowControl {
	jobQueue := NewJobQueue(10)
	fmt.Println("init job queue success")

	m := NewWorkerManager(jobQueue)
	m.createWorker()
	fmt.Println("init worker success")

	control := &FlowControl{
		jobQueue: jobQueue,
		wm:       m,
	}
	fmt.Println("init flowcontrol success")
	return control
}

func (c *FlowControl) CommitJob(job *Job) {
	c.jobQueue.PushJob(job)
	fmt.Println("commit job success")
}

type Job struct {
	DoneChan  chan struct{}
	handleJob func(j *Job) error
}

func (job *Job) Done() {
	job.DoneChan <- struct{}{}
	close(job.DoneChan)
}

func (job *Job) WaitDone() {
	select {
	case <-job.DoneChan:
		return
	}
}

func (job *Job) Execute() error {
	fmt.Println("job start to execute ")
	return job.handleJob(job)
}

type JobQueue struct {
	mu         sync.Mutex
	noticeChan chan struct{}
	queue      *list.List
	size       int
	capacity   int
}

func NewJobQueue(cap int) *JobQueue {
	return &JobQueue{
		capacity: cap,
		queue:    list.New(),
		noticeChan: make(chan struct{}, 1),
	}
}

func (q *JobQueue) PushJob(job *Job) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.size++
	if q.size > q.capacity {
		q.RemoveLeastJob()
	}

	q.queue.PushBack(job)


	q.noticeChan <- struct{}{}
}

func (q *JobQueue) PopJob() *Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.size == 0 {
		return nil
	}

	q.size--
	return q.queue.Remove(q.queue.Front()).(*Job)
}

func (q *JobQueue) RemoveLeastJob() {
	if q.queue.Len() != 0 {
		back := q.queue.Back()
		abandonJob := back.Value.(*Job)
		abandonJob.Done()
		q.queue.Remove(back)
	}
}

func (q *JobQueue) waitJob() <-chan struct{} {
	return q.noticeChan
}

type WorkerManager struct {
	jobQueue *JobQueue
}

func NewWorkerManager(jobQueue *JobQueue) *WorkerManager {
	return &WorkerManager{
		jobQueue: jobQueue,
	}
}

func (m *WorkerManager) createWorker() error {

	go func() {
		fmt.Println("start the worker success")
		var job *Job

		for {
			select {
			case <-m.jobQueue.waitJob():
				fmt.Println("get a job from job queue")
				job = m.jobQueue.PopJob()
				fmt.Println("start to execute job")
				job.Execute()
				fmt.Print("execute job done")
				job.Done()
			}
		}
	}()

	return nil
}
