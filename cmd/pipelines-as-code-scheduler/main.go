package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/sm43/pac-scheduler/pkg/sync"
	pipelineClientSet "github.com/tektoncd/pipeline/pkg/client/clientset/versioned"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

var freq = map[string]int{
	"repo-abc": 3,
	"repo-xyz": 2,
	"repo-one": 1,
}

var getSyncLimit = func(lockKey string) (int, error) {
	arr := strings.Split(lockKey, "/")
	log.Println("arr element = ", arr[1])
	limit, ok := freq[arr[1]]
	if ok {
		return limit, nil
	}
	return 0, nil
}

var isSDDeleted = func(key string) bool {
	return false
}

func main() {

	clusterConfig, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("Failed to get in cluster config: %v", err)
	}

	pipelineCS, err := pipelineClientSet.NewForConfig(clusterConfig)
	if err != nil {
		log.Fatalf("Failed to create pipeline client set: %v", err)
	}

	manager := sync.NewLockManager(getSyncLimit, isSDDeleted)

	router := mux.NewRouter()

	router.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		fmt.Fprint(w, "okay!")
	})

	s := &scheduler{
		Manager:        manager,
		PipelineClient: pipelineCS,
	}

	registerEvent := http.HandlerFunc(s.register)
	v := &validator{Handler: registerEvent}
	router.HandleFunc("/register/pipelinerun/{namespace}/{name}", v.validateAndExecute())

	if err := http.ListenAndServe(":8080", router); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
}

type validator struct {
	Handler http.Handler
}

func (v *validator) validateAndExecute() http.HandlerFunc {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		log.Println("validating payload....")
		log.Println("now serving...")
		v.Handler.ServeHTTP(response, request)
	})
}

type scheduler struct {
	Manager        *sync.Manager
	PipelineClient pipelineClientSet.Interface
}

func (s *scheduler) register(response http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	prNamespace, ok := vars["namespace"]
	if !ok {
		response.WriteHeader(http.StatusBadRequest)
		return
	}

	prName, ok := vars["name"]
	if !ok {
		response.WriteHeader(http.StatusBadRequest)
		return
	}

	log.Printf("-----------------------------------------------In")

	log.Printf("request for register: %s/%s", prNamespace, prName)

	// validate pr and add in the queue
	pr, err := s.PipelineClient.TektonV1beta1().PipelineRuns(prNamespace).Get(context.Background(), prName, v1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			log.Println("pipelinerun doesn't exist : ", prName, prNamespace)
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		log.Printf("failed to get pipelinerun: %v", err)
		response.WriteHeader(http.StatusInternalServerError)
		return
	}

	repoAnno := "pipelinesascode.tekton.dev/repositoryName"
	repoName, ok := pr.GetAnnotations()[repoAnno]
	if !ok {
		response.WriteHeader(http.StatusBadRequest)
		return
	}

	// check if the pipelineRun has pending status

	e := &sync.Event{
		PipelineRunName: pr.Name,
		RepoName:        repoName,
		Namespace:       pr.Namespace,
		EventTime:       time.Time{},
	}

	got, err := s.Manager.Register(e, s.PipelineClient)
	if err != nil {
		log.Printf("failed to register: %v", err)
		response.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.Println("...", got)

	s.Manager.Print(e)

	response.WriteHeader(http.StatusOK)
	return
}
