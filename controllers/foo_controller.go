/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"encoding/json"

	"k8s.io/apimachinery/pkg/runtime"
	tutorialv1 "my.domain/tutorial/api/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/maniknutanix/k8-grpc/juno_manager_client"
)

// TODO: Move to a shared package..

var (
	OperationStatus_COMPLETED           = "COMPLETED"
	OperationStatus_IN_PROGRESS         = "IN_PROGRESS"
	OperationStatus_CREATE_REQUEST_SENT = "CREATE_REQUEST_SENT"
	OperationStatus_INIT                = "" // Default value for a new custom resource.
)

// FooReconciler reconciles a Foo object
type FooReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	juno_manager_client.JunoManagerIfc
}

//+kubebuilder:rbac:groups=tutorial.my.domain,resources=foos,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=tutorial.my.domain,resources=foos/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=tutorial.my.domain,resources=foos/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Foo object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.12.2/pkg/reconcile
func (r *FooReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("reconciling foo custom resource")

	// Get the Foo resource that triggered the reconciliation request
	var foo tutorialv1.Foo
	if err := r.Get(ctx, req.NamespacedName, &foo); err != nil {
		log.Error(err, "unable to fetch Foo")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if foo.Status.OperationStatus == OperationStatus_COMPLETED {
		// Nothing to do.
		return ctrl.Result{}, nil
	}

	var updateStatusBeforeReturn bool
	requestID := string(foo.ObjectMeta.UID)
	if foo.Status.OperationStatus == OperationStatus_INIT {
		// If requestID is not present in status, this is a new request to create snapshot.
		log.V(1).Info("new request received for creating snapshot")
		_, err := r.CreateSnapshot(ctx, requestID)
		if err != nil {
			return ctrl.Result{}, err
		}

		// Update operation status to indicate that the create request has been sent.
		foo.Status.OperationStatus = OperationStatus_CREATE_REQUEST_SENT
		updateStatusBeforeReturn = true
	}

	_, createRequestSent, err := r.GetResponse(requestID)
	if err != nil {
		if updateStatusBeforeReturn {
			if err := r.Status().Update(ctx, &foo); err != nil {
				log.Error(err, "unable to update foo's requestID status", "requestID", requestID)
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, err
	}

	if !createRequestSent {
		// Subscribe for notifications for this request ID.
		log.Info("Subscribing for create snapshot updates", "requestID", requestID)
		callbackObj := juno_manager_client.NewCallbackObject(r.NotifyCallback, ctx, req, requestID)
		r.Subscribe(ctx, requestID, callbackObj)
		// We're done. Every notification from server will call the NotifyCallback function.
		return ctrl.Result{}, nil
	}

	log.Info("Nothing to do. Waiting for notifications from server")
	return ctrl.Result{}, nil
}

func (r *FooReconciler) NotifyCallback(ctx context.Context, req ctrl.Request, requestID string, response *juno_manager_client.NotifyProgressResponse) {
	log := log.FromContext(ctx)
	// Get the live Foo resource that triggered the reconciliation request
	var foo tutorialv1.Foo
	if err := r.Get(ctx, req.NamespacedName, &foo); err != nil {
		log.Error(err, "unable to fetch Foo")
		return
	}

	if r.HasRequestCompleted(response) {
		// If the request has been marked complete, unsubscribe for further
		// updates and remove the requestID from the global statusMap.
		// Update any relevant information provided by the server in the status.
		log.V(1).Info("The request to create snapshot is complete, unsubscribing updates...")
		r.Unsubscribe(ctx, requestID)

		foo.Status.OperationStatus = OperationStatus_COMPLETED
		// foo.Status.RptIDs = r.JunoManagerClient.StatusMap[requestID].RptIDs
		if err := r.Status().Update(ctx, &foo); err != nil {
			log.Error(err, "unable to update foo's ready status", "requestID", requestID)
			return
		}

		log.Info("foo custom resource reconciled")
		return
	}

	log.Info("The request with request ID hasn't completed yet. Will wake up at next notification from server", "requestID", requestID)
}

func JsonObjectToString(obj interface{}) string {
	jsonBytes, err := json.Marshal(obj)
	if err != nil {
		return "unable to marshal object"
	}

	return string(jsonBytes)
}

// SetupWithManager sets up the controller with the Manager.
func (r *FooReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tutorialv1.Foo{}).
		Complete(r)
}
