add_library(chaos
        chaos.hpp
        chaos.cpp
        )

target_link_libraries(chaos options)

add_subdirectory(test)
