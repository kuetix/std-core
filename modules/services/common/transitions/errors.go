package transitions

import (
	"net/http"
	"time"

	"github.com/kuetix/engine/engine/domain"
	"github.com/kuetix/engine/engine/domain/interfaces"
	"github.com/kuetix/engine/engine/workflow"
	"github.com/kuetix/logger"
)

type errorsTransitions struct {
	workflow.BaseServiceTransition
}

func NewErrorsTransitions() interfaces.ServiceTransitions {
	return &errorsTransitions{}
}

// OnErrorHook, when non-nil, is called from OnAnyError with the cleaned
// error message and the workflow it failed in. This package is reusable
// across consumers, so it never picks an error-tracking vendor itself -
// a consuming binary wires its own sink (e.g. Sentry) into this hook at
// startup, the same composable package-level-hook shape std-http's
// ServerHandlerWrapper already uses. nil (the default) is a no-op, so a
// binary that never sets it behaves exactly as before.
var OnErrorHook func(msg string, workflow string)

func (et *errorsTransitions) OnAnyError(msg string, statusCode int) (r domain.FlowStepResult) {
	et.SetStatusCode(statusCode)
	res := et.Ctx.Worker.GetWorkerResponse()
	issues := res.GetError()
	if issues != nil {
		msg = issues.Error()
	}
	if OnErrorHook != nil {
		workflowName := ""
		if et.Ctx != nil && et.Ctx.Flow != nil {
			workflowName = et.Ctx.Flow.Name
		}
		OnErrorHook(msg, workflowName)
	}
	et.Ctx.Worker.CleanErrors()
	et.SetValue("error", map[string]interface{}{"message": msg})

	r.Success = true
	return
}

func (et *errorsTransitions) LogError(level string, notify string) (r domain.FlowStepResult) {
	res := et.Ctx.Worker.GetWorkerResponse()
	msg := "unknown error"
	issues := res.GetError()
	if issues != nil {
		msg = issues.Error()
	}

	switch level {
	case "debug":
		logger.Debug(msg)
	case "info":
		logger.Info(msg)
	case "warn", "warning":
		logger.Warn(msg)
	default:
		logger.Error(msg)
	}

	et.Ctx.Worker.CleanErrors()
	et.SetValue("error", map[string]interface{}{
		"message": msg,
		"level":   level,
		"notify":  notify,
	})

	r.Success = true
	return
}

func (et *errorsTransitions) RetryWithBackoff(maxRetries int, backoffMs int) (r domain.FlowStepResult) {
	res := et.Ctx.Worker.GetWorkerResponse()
	msg := "unknown error"
	if res.Error != nil {
		msg = res.GetError().Error()
	}

	retryState := ""
	if et.Ctx != nil && et.Ctx.Flow != nil {
		if et.Ctx.Flow.LastTrace != nil {
			retryState = et.Ctx.Flow.LastTrace.GetFrom()
		}
		if (retryState == "" || retryState == "_") && et.Ctx.Flow.CurrentTransition != nil && len(et.Ctx.Flow.CurrentTransition.From) > 0 {
			retryState = et.Ctx.Flow.CurrentTransition.From[0]
		}
	}
	if retryState == "_" {
		retryState = ""
	}

	retryValues := map[string]interface{}{}
	if existing := et.Ctx.Value("retry"); existing != nil {
		if existingMap, ok := existing.(map[string]interface{}); ok {
			retryValues = existingMap
		} else if existingMap, ok := existing.(map[string]int); ok {
			for key, value := range existingMap {
				retryValues[key] = value
			}
		}
	}

	attempts := 0
	if retryState != "" {
		if value, ok := retryValues[retryState]; ok {
			switch typed := value.(type) {
			case int:
				attempts = typed
			case int64:
				attempts = int(typed)
			case float64:
				attempts = int(typed)
			case float32:
				attempts = int(typed)
			}
		}
	}
	attempts++

	shouldRetry := retryState != "" && attempts <= maxRetries
	if shouldRetry && backoffMs > 0 {
		delay := time.Duration(backoffMs) * time.Millisecond
		for i := 1; i < attempts; i++ {
			delay *= 2
		}
		time.Sleep(delay)
	}

	if retryState != "" {
		retryValues[retryState] = attempts
		et.SetValue("retry", retryValues)
	}

	et.SetValue("error", map[string]interface{}{
		"message": msg,
		"retry": map[string]interface{}{
			"attempt":    attempts,
			"maxRetries": maxRetries,
			"backoffMs":  backoffMs,
			"state":      retryState,
			"retrying":   shouldRetry,
		},
	})

	if shouldRetry {
		et.Ctx.Worker.CleanErrors()
		r.Success = true
		r.Next = retryState
		return
	}

	r.Success = false
	return
}

func (et *errorsTransitions) CriticalFailure(notify string, escalate bool) (r domain.FlowStepResult) {
	res := et.Ctx.Worker.GetWorkerResponse()
	msg := "unknown error"
	issues := res.GetError()
	if issues != nil {
		msg = issues.Error()
	}

	logger.Error(msg)

	if res.GetStatusCode() == 0 {
		et.SetStatusCode(http.StatusInternalServerError)
	}

	et.SetValue("error", map[string]interface{}{
		"message":  msg,
		"level":    "critical",
		"notify":   notify,
		"escalate": escalate,
	})

	r.Success = true
	return
}

func (et *errorsTransitions) CleanupErrors() (r domain.FlowStepResult) {
	et.Ctx.Worker.CleanErrors()

	r.Success = true
	return
}
