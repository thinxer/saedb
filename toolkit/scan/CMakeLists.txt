project(scan)
file(GLOB scan_sources "*.cpp")
add_executable(scan ${scan_sources})
target_link_libraries(scan ${SGRAPH_BASE})
