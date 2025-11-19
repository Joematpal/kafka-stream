package main

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
)

// MergePatch applies a patch using zero values for deletion (like omitempty)
func MergePatch(target, patch []byte) ([]byte, error) {
	var t, p interface{}

	if err := json.Unmarshal(target, &t); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(patch, &p); err != nil {
		return nil, err
	}

	result := merge(t, p)
	return json.Marshal(result)
}

func merge(target, patch interface{}) interface{} {
	// If patch isn't an object, just return the patch
	patchObj, ok := patch.(map[string]interface{})
	if !ok {
		return patch
	}

	// If target isn't an object, start with empty object
	targetObj, ok := target.(map[string]interface{})
	if !ok {
		targetObj = make(map[string]interface{})
	}

	// Copy target
	result := make(map[string]interface{})
	for k, v := range targetObj {
		result[k] = v
	}

	// Apply patch
	for k, v := range patchObj {
		if isZeroValue(v) {
			delete(result, k) // zero value means delete (like omitempty)
		} else {
			result[k] = merge(result[k], v) // recursive merge
		}
	}

	return result
}

// isZeroValue checks if a value is a zero value (like omitempty would omit)
func isZeroValue(v interface{}) bool {
	if v == nil {
		return true
	}

	val := reflect.ValueOf(v)
	switch val.Kind() {
	case reflect.String:
		return val.String() == ""
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return val.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return val.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return val.Float() == 0
	case reflect.Bool:
		return !val.Bool()
	case reflect.Slice, reflect.Map, reflect.Array:
		return val.Len() == 0
	default:
		return false
	}
}

// Struct-based approach for type safety
type User struct {
	Name    string `json:"name,omitempty"`
	Age     int    `json:"age,omitempty"`
	City    string `json:"city,omitempty"`
	Country string `json:"country,omitempty"`
}

// MergeStruct merges structs using omitempty behavior
func MergeStruct[T any](target T, patch T) (T, error) {
	// Convert to JSON and back to apply omitempty logic
	targetJSON, err := json.Marshal(target)
	if err != nil {
		return target, err
	}

	patchJSON, err := json.Marshal(patch)
	if err != nil {
		return target, err
	}

	// Apply merge patch
	resultJSON, err := MergePatch(targetJSON, patchJSON)
	if err != nil {
		return target, err
	}

	// Unmarshal back to struct
	var result T
	err = json.Unmarshal(resultJSON, &result)
	return result, err
}

// Merge is a generic function that merges any two structs using omitempty behavior
// Zero values in the patch will delete fields from the target (like omitempty)
func Merge[T any](target T, patch T) (T, error) {
	return MergeStruct(target, patch)
}

// MustMerge is like Merge but panics on error
func MustMerge[T any](target T, patch T) T {
	result, err := Merge(target, patch)
	if err != nil {
		panic(err)
	}
	return result
}

// MergeInto merges patch into target in-place by updating target's pointer
func MergeInto[T any](target *T, patch T) error {
	result, err := Merge(*target, patch)
	if err != nil {
		return err
	}
	*target = result
	return nil
}

func main() {
	// Example 1: JSON approach with zero values as delete
	fmt.Println("=== JSON Merge Patch with Zero Values ===")
	target := `{"name": "John", "age": 30, "city": "NYC"}`
	patch := `{"age": 31, "city": "", "country": "USA"}` // empty string deletes city

	result, err := MergePatch([]byte(target), []byte(patch))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Target:  %s\n", target)
	fmt.Printf("Patch:   %s\n", patch)
	fmt.Printf("Result:  %s\n", result)

	// Example 2: Generic Merge function (recommended)
	fmt.Println("\n=== Generic Merge Function ===")
	user := User{Name: "John", Age: 30, City: "NYC"}
	userPatch := User{Age: 31, Country: "USA"} // City omitted = delete

	mergedUser, err := Merge(user, userPatch)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Original: %+v\n", user)
	fmt.Printf("Patch:    %+v\n", userPatch)
	fmt.Printf("Merged:   %+v\n", mergedUser)

	// Example 3: MustMerge (panics on error)
	fmt.Println("\n=== MustMerge (no error handling) ===")
	user2 := User{Name: "Jane", Age: 25, City: "LA", Country: "USA"}
	patch2 := User{Name: "Jane", Age: 26} // Remove city and country
	merged2 := MustMerge(user2, patch2)

	fmt.Printf("Original: %+v\n", user2)
	fmt.Printf("Patch:    %+v\n", patch2)
	fmt.Printf("Merged:   %+v\n", merged2)

	// Example 4: MergeInto (in-place modification)
	fmt.Println("\n=== MergeInto (in-place) ===")
	user3 := User{Name: "Bob", Age: 40, City: "Chicago"}
	patch3 := User{Age: 41, Country: "Canada"} // Update age, remove city, add country

	fmt.Printf("Before:   %+v\n", user3)
	err = MergeInto(&user3, patch3)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("After:    %+v\n", user3)
}
