file(GLOB_RECURSE SOLUTION_SRC CONFIGURE_DEPENDS "executor/*.cpp")
add_shad_library(executor ${SOLUTION_SRC})

add_catch(test_executor test_executor.cpp test_future.cpp)
target_link_libraries(test_executor PRIVATE executor)

add_catch(bench_executor run.cpp)
target_link_libraries(bench_executor PRIVATE executor)
