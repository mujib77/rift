package filter

import (
	"fmt"
	"os"

	"github.com/dop251/goja"
	"github.com/mujib77/rift/internal/source"
)

type Filter struct {
	vm     *goja.Runtime
	script string
}

func New(scriptPath string) (*Filter, error) {
	if scriptPath == "" {
		return nil, nil
	}

	data, err := os.ReadFile(scriptPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read filter script: %w", err)
	}

	vm := goja.New()
	_, err = vm.RunString(string(data))
	if err != nil {
		return nil, fmt.Errorf("failed to compile filter script: %w", err)
	}

	fmt.Println("  filter script loaded:", scriptPath)
	return &Filter{vm: vm, script: scriptPath}, nil
}

func (f *Filter) Allow(event *source.Event) (bool, error) {
	if f == nil {
		return true, nil
	}

	filterFn, ok := goja.AssertFunction(f.vm.Get("filter"))
	if !ok {
		return true, fmt.Errorf("filter function not found in script")
	}

	eventObj := f.vm.NewObject()
	eventObj.Set("table", event.Table)
	eventObj.Set("operation", event.Operation)
	eventObj.Set("lsn", event.LSN)

	dataObj := f.vm.NewObject()
	for k, v := range event.Data {
		dataObj.Set(k, v)
	}
	eventObj.Set("data", dataObj)

	result, err := filterFn(goja.Undefined(), eventObj)
	if err != nil {
		return true, fmt.Errorf("filter script error: %w", err)
	}

	return result.ToBoolean(), nil
}