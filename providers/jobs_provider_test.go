package providers

import (
	"reflect"
	"testing"
)

func TestGetAllJobs(t *testing.T) {
	var expected = []JobsConfigs{}
	providers = []JobsConfigs{}

	returned := GetAllJobs()

	if !reflect.DeepEqual(expected, providers) {
		t.Errorf("Expected that value returned is type []JobsConfigs but got %v", reflect.TypeOf(returned))
	}
}
