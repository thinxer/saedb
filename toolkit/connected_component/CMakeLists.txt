project(connected_component)
add_executable(connected_component "connected_component.cpp")
target_link_libraries(connected_component ${SGRAPH_BASE})
