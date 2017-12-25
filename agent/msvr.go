package agent

import (
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/standardcore/Matcha/api"
	"github.com/standardcore/Matcha/backgroundjob"
	"github.com/standardcore/Matcha/essentials"
	"github.com/standardcore/Matcha/rtevent"
	"github.com/standardcore/go-collections"
)

type MServer struct {
	sess *essentials.Session

	once collections.List

	httpServer   *http.Server
	httpProtocol string
	httpAddress  string

	servicePrefix string

	infiniteProcessor *essentials.InfiniteProcessor
}

func NewMServer(sess *essentials.Session) *MServer {
	return &MServer{
		sess:              sess,
		once:              collections.NewList(),
		httpServer:        new(http.Server),
		infiniteProcessor: essentials.NewInfiniteProcessor(sess),
	}
}

func (s *MServer) ServicePrefix(prefix string) *MServer {
	s.servicePrefix = prefix
	return s
}

func (s *MServer) StartUp() error {
	//RabbitMQ Middlewares
	//s.once.Add(essentials.NewConfirmCallbackMiddleware(s.sess))
	s.once.Add(backgroundjob.NewFailSafeMiddleware(s.sess))
	//HTTP
	//s.http["/check"] = NewCheckMiddleware(s.sess)
	// s.http[fmt.Sprintf("%s/v1/check", s.servicePrefix)] = s.ParseHTTPFunc(func(_ *essentials.MatchaContext, w http.ResponseWriter, _ *http.Request) error {
	// 	w.WriteHeader(204)
	// 	return nil
	// })
	// s.http[fmt.Sprintf("%s/v1/job/create", s.servicePrefix)] = backgroundjob.NewAddJobMiddleware(s.sess)
	// s.http[fmt.Sprintf("%s/v1/changestate", s.servicePrefix)] = essentials.NewChangeStateMiddleware(s.sess)
	// s.http[fmt.Sprintf("%s/v1/event/publish", s.servicePrefix)] = rtevent.NewPublishEventMiddleware(s.sess)
	return nil
}

func (s *MServer) Configure() error {
	err := s.configureConsumers()
	if err != nil {
		return err
	}
	err = s.configureHTTP()
	if err != nil {
		return err
	}

	err = backgroundjob.RebuildScheduler(s.sess)
	if err != nil {
		return err
	}

	s.infiniteProcessor.Process()

	return nil
}

func (s *MServer) HTTPServer() *http.Server {
	return s.httpServer
}

func (s *MServer) configureConsumers() error {
	err := collections.TryForEach(s.once, func(m interface{}) error {
		err := m.(essentials.Middleware).Execute(essentials.NewMatchaContext(s.sess))
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {

		return err
	}
	return nil
}

func (s *MServer) configureHTTP() error {
	r := mux.NewRouter()
	r.HandleFunc("/check", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(204)
	}).Methods(http.MethodGet)

	r.HandleFunc("/v1/changestate", func(writer http.ResponseWriter, request *http.Request) {
		content, err := ioutil.ReadAll(request.Body)
		if err != nil {
			writer.WriteHeader(500)
			writer.Write([]byte(err.Error()))
			return
		}
		err = essentials.ExecuteChangeState(content, s.sess)
		if err != nil {
			writer.WriteHeader(500)
			writer.Write([]byte(err.Error()))
			return
		}
		writer.WriteHeader(204)
	}).Methods(http.MethodPost)

	r.HandleFunc("/v1/job/create", func(writer http.ResponseWriter, request *http.Request) {
		content, err := ioutil.ReadAll(request.Body)
		if err != nil {
			writer.WriteHeader(500)
			writer.Write([]byte(err.Error()))
			return
		}
		msgid, err := backgroundjob.ExecuteAddJob(content, request, s.sess)
		if err != nil {
			writer.WriteHeader(500)
			writer.Write([]byte(err.Error()))
			return
		}
		writer.WriteHeader(200)
		writer.Write([]byte(msgid))
	}).Methods(http.MethodPost)

	r.HandleFunc("/v1/event/publish", func(writer http.ResponseWriter, request *http.Request) {
		content, err := ioutil.ReadAll(request.Body)
		if err != nil {
			writer.WriteHeader(500)
			writer.Write([]byte(err.Error()))
			return
		}
		err = rtevent.ExecutePublishEvent(content, request, s.sess)
		if err != nil {
			writer.WriteHeader(500)
			writer.Write([]byte(err.Error()))
			return
		}
		writer.WriteHeader(204)
	}).Methods(http.MethodPost)

	r.HandleFunc("/v1/api/listevents", func(writer http.ResponseWriter, request *http.Request) {
		content, err := ioutil.ReadAll(request.Body)
		if err != nil {
			writer.WriteHeader(500)
			writer.Write([]byte(err.Error()))
			return
		}
		body, err := api.ExecuteListEvents(content, s.sess)
		if err != nil {
			writer.WriteHeader(500)
			writer.Write([]byte(err.Error()))
			return
		}
		if body == nil {
			writer.WriteHeader(204)
		} else {
			writer.WriteHeader(200)
			writer.Write(body)
		}
	}).Methods(http.MethodPost).Headers("Content-Type", "application/json")

	r.HandleFunc("/v1/api/listjobs", func(writer http.ResponseWriter, request *http.Request) {
		content, err := ioutil.ReadAll(request.Body)
		if err != nil {
			writer.WriteHeader(500)
			writer.Write([]byte(err.Error()))
			return
		}
		body, err := api.ExecuteListJobs(content, s.sess)
		if err != nil {
			writer.WriteHeader(500)
			writer.Write([]byte(err.Error()))
			return
		}
		if body == nil {
			writer.WriteHeader(204)
		} else {
			writer.WriteHeader(200)
			writer.Write(body)
		}
	}).Methods(http.MethodPost).Headers("Content-Type", "application/json")

	r.HandleFunc("/v1/api/getcontent", func(writer http.ResponseWriter, request *http.Request) {
		messageid := request.URL.Query().Get("id")
		content, err := api.ExecuteGetMessageContent(messageid, s.sess)
		if err != nil {
			writer.WriteHeader(500)
			writer.Write([]byte(err.Error()))
			return
		}
		if content == nil {
			writer.WriteHeader(204)
		} else {
			writer.WriteHeader(200)
			writer.Write(content)
		}
	}).Methods(http.MethodGet)

	s.HTTPServer().Handler = r
	return nil
}

func (s *MServer) ParseHTTPFunc(httpHandler func(*essentials.MatchaContext, http.ResponseWriter, *http.Request) error) essentials.RoutedMiddleware {
	return essentials.NewCommonHTTPMiddleware(s.sess, &httpHandler)
}
