package cmd

import (
	"fmt"
	"log"
	"net/http"
	"path/filepath"

	"github.com/odpf/meteor/api"
	"github.com/odpf/meteor/extractors"
	"github.com/odpf/meteor/processors"
	"github.com/odpf/meteor/recipes"
	"github.com/odpf/meteor/sinks"
)

var (
	PORT = "3000"
)

func Serve() {
	recipeStore := initRecipeStore()
	extractorStore := initExtractorStore()
	processorStore := initProcessorStore()
	sinkStore := initSinkStore()

	recipeService := recipes.NewService(
		recipeStore,
		extractorStore,
		processorStore,
		sinkStore,
	)
	recipeHandler := api.NewRecipeHandler(recipeService)

	router := api.NewRouter()
	api.SetupRoutes(router, recipeHandler)

	fmt.Println("Listening on port :" + PORT)
	err := http.ListenAndServe(":"+PORT, router)
	if err != nil {
		fmt.Println(err)
	}
}
func initRecipeStore() recipes.Store {
	path, err := filepath.Abs("./")
	if err != nil {
		log.Fatal(err.Error())
	}
	path = filepath.Join(path, "_recipes")
	reader := recipes.NewReader(path)
	recipeList, err := reader.Read()
	if err != nil {
		log.Fatal(err.Error())
	}
	store := recipes.NewMemoryStore()
	for _, r := range recipeList {
		store.Create(r)
	}
	return store
}
func initExtractorStore() *extractors.Store {
	store := extractors.NewStore()
	extractors.PopulateStore(store)
	return store
}
func initProcessorStore() *processors.Store {
	store := processors.NewStore()
	processors.PopulateStore(store)
	return store
}
func initSinkStore() *sinks.Store {
	store := sinks.NewStore()
	sinks.PopulateStore(store)
	return store
}