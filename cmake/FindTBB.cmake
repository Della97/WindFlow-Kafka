# Try to find INTEL TBB libraries

if(NOT TBB_FIND_QUIETLY)
    set(_TBB_FIND_STATUS "Looking for TBB")
    if(TBB_FIND_COMPONENTS)
        set(_TBB_FIND_STATUS "${_TBB_FIND_STATUS} [${TBB_FIND_COMPONENTS}]")
    endif()
    if(NOT TBB_FIND_REQUIRED)
        set(_TBB_FIND_STATUS "${_TBB_FIND_STATUS} (optional)")
    endif()
    message(STATUS "${_TBB_FIND_STATUS}...")
endif()

# Default required/optional components
if(NOT TBB_FIND_COMPONENTS)
    set(TBB_FIND_COMPONENTS tbb malloc malloc_proxy)
    set(TBB_FIND_REQUIRED_tbb          TRUE)
    set(TBB_FIND_REQUIRED_malloc       FALSE)
    set(TBB_FIND_REQUIRED_malloc_proxy FALSE)
endif()

# Normalize component names
set(_TBB_FIND_COMPONENTS)
foreach(__TBB_COMPONENT IN LISTS TBB_FIND_COMPONENTS)
    string(TOUPPER "${__TBB_COMPONENT}" _TBB_COMPONENT)
    string(REGEX REPLACE "^TBB_?([A-Z_]+)$" "\\1" _TBB_COMPONENT "${_TBB_COMPONENT}")
    if(_TBB_COMPONENT MATCHES "^(TBB|MALLOC|MALLOC_PROXY)$")
        set(_TBB_${_TBB_COMPONENT}_NAME ${__TBB_COMPONENT})
        list(APPEND _TBB_FIND_COMPONENTS ${_TBB_COMPONENT})
        if(TBB_FIND_REQUIRED_${__TBB_COMPONENT})
            set(_TBB_FIND_REQUIRED_${_TBB_COMPONENT} TRUE)
        else()
            set(_TBB_FIND_REQUIRED_${_TBB_COMPONENT} FALSE)
        endif()
    else()
        message(FATAL_ERROR "Unknown TBB library component: ${__TBB_COMPONENT}\n"
                        "Valid component names are: tbb, [tbb]malloc, [tbb]malloc_proxy")
    endif()
endforeach ()
unset(__TBB_COMPONENT)

if(TBB_DEBUG)
    message("** FindTBB: Components = [${_TBB_FIND_COMPONENTS}]")
endif()

# Names of headers and libraries for each component
set(_TBB_TBB_LIB_NAMES_RELEASE          tbb)
set(_TBB_TBB_LIB_NAMES_DEBUG            tbb_debug)
set(_TBB_TBB_INC_NAMES                  tbb/tbb.h)
set(_TBB_MALLOC_LIB_NAMES_RELEASE       tbbmalloc)
set(_TBB_MALLOC_LIB_NAMES_DEBUG         tbbmalloc_debug)
set(_TBB_MALLOC_INC_NAMES               tbb/tbb.h)
set(_TBB_MALLOC_PROXY_LIB_NAMES_RELEASE tbbmalloc_proxy)
set(_TBB_MALLOC_PROXY_LIB_NAMES_DEBUG   tbbmalloc_proxy_debug)
set(_TBB_MALLOC_PROXY_INC_NAMES         tbb/tbbmalloc_proxy.h)

# Transitive link dependencies
set(_TBB_TBB_LIB_LINK_DEPENDS)
set(_TBB_MALLOC_LIB_LINK_DEPENDS)
set(_TBB_MALLOC_PROXY_LIB_LINK_DEPENDS)
if(UNIX AND NOT APPLE)
    # On Linux, the TBB threading library requires librt.so
    list(APPEND _TBB_TBB_LIB_LINK_DEPENDS rt)
endif()

# Construct a set of search paths
set(_TBB_ARCH_PLATFORM $ENV{TBB_ARCH_PLATFORM})
if(NOT TBB_ROOT)
    file(TO_CMAKE_PATH "$ENV{TBB_ROOT}" TBB_ROOT)
endif()

set(_TBB_INC_PATH_SUFFIXES include)
set(_TBB_LIB_PATH_SUFFIXES)
if(_TBB_ARCH_PLATFORM)
    list(APPEND _TBB_LIB_PATH_SUFFIXES lib/${_TBB_ARCH_PLATFORM})
    list(APPEND _TBB_LIB_PATH_SUFFIXES ${_TBB_ARCH_PLATFORM}/lib)
endif()
list(APPEND _TBB_LIB_PATH_SUFFIXES lib)

if(WIN32 AND MSVC AND CMAKE_GENERATOR MATCHES "Visual Studio ([0-9]+)")
    set(_TBB_MSVS_VERSION ${CMAKE_MATCH_1})
    if(CMAKE_CL_64)
        list(APPEND _TBB_LIB_PATH_SUFFIXES lib/intel64/vc${_TBB_MSVS_VERSION})
        list(APPEND _TBB_LIB_PATH_SUFFIXES intel64/vc${_TBB_MSVS_VERSION}/lib)
        list(APPEND _TBB_LIB_PATH_SUFFIXES lib/ia64/vc${_TBB_MSVS_VERSION})
        list(APPEND _TBB_LIB_PATH_SUFFIXES ia64/vc${_TBB_MSVS_VERSION}/lib)
    else()
        list(APPEND _TBB_LIB_PATH_SUFFIXES lib/ia32/vc${_TBB_MSVS_VERSION})
        list(APPEND _TBB_LIB_PATH_SUFFIXES ia32/vc${_TBB_MSVS_VERSION}/lib)
    endif()
    unset(_TBB_MSVS_VERSION)
endif()

if(TBB_DEBUG)
    message("** FindTBB: Initial search paths:")
    message("** FindTBB: - Root directory hints  = [${TBB_ROOT}]")
    message("** FindTBB: - Include path suffixes = [${_TBB_INC_PATH_SUFFIXES}]")
    message("** FindTBB: - Library path suffixes = [${_TBB_LIB_PATH_SUFFIXES}]")
endif()

# Find common include directory 
find_path(TBB_INCLUDE_DIR
  NAMES tbb/tbb_stddef.h
  HINTS ${TBB_ROOT}
  PATH_SUFFIXES ${_TBB_INC_PATH_SUFFIXES}
)

mark_as_advanced(TBB_INCLUDE_DIR)

# Derive TBB_ROOT from TBB_INCLUDE_DIR if unset
if(TBB_INCLUDE_DIR AND NOT TBB_ROOT)
    if(_TBB_INC_PATH_SUFFIXES MATCHES "[^/;]/[^/;]")
        string(LENGTH "${TBB_INCLUDE_DIR}" _TBB_INCLUDE_DIR_LENGTH)
        foreach(_TBB_INC_PATH_SUFFIX IN LISTS _TBB_INC_PATH_SUFFIXES)
            string(LENGTH "${_TBB_INC_PATH_SUFFIX}" _TBB_INC_PATH_SUFFIX_LENGTH)
            if(_TBB_INC_PATH_SUFFIX_LENGTH GREATER 0)
                math(EXPR _TBB_SUBSTRING_START "${_TBB_INCLUDE_DIR_LENGTH} - ${_TBB_INC_PATH_SUFFIX_LENGTH}")
                string(SUBSTRING "${TBB_INCLUDE_DIR}" _TBB_SUBSTRING_START -1 _TBB_SUBSTRING)
                if(_TBB_SUBSTRING STREQUAL _TBB_INC_PATH_SUFFIX)
                    if(_TBB_SUBSTRING_START GREATER 0)
                        string(SUBSTRING "${TBB_INCLUDE_DIR}" 0 _TBB_SUBSTRING_START TBB_ROOT)
                        string(REGEX REPLACE "/+$" "" TBB_ROOT "${TBB_ROOT}")
                    else()
                    set(TBB_ROOT "/")
                endif()
                break()
            endif()
        endif()
    endforeach ()
    unset(_TBB_SUBSTRING)
    unset(_TBB_SUBSTRING_START)
    unset(_TBB_INCLUDE_DIR_LENGTH)
    unset(_TBB_INC_PATH_SUFFIX_LENGTH)
    else()
        get_filename_component(TBB_ROOT "${TBB_INCLUDE_DIR}" DIRECTORY)
    endif()
endif()

if(TBB_DEBUG)
    message("** FindTBB: After initial search of TBB include path")
    message("** FindTBB: - TBB_INCLUDE_DIR = ${TBB_INCLUDE_DIR}")
    message("** FindTBB: - TBB_ROOT        = [${TBB_ROOT}]")
endif()

# Find library components
set(TBB_INCLUDE_DIRS)
set(TBB_LIBRARIES)

foreach(_TBB_COMPONENT IN LISTS _TBB_FIND_COMPONENTS)
    if(TBB_DEBUG)
        message("** FindTBB: Looking for component ${_TBB_COMPONENT}...")
    endif()

    # Find include path and library files of this component
    find_path(TBB_${_TBB_COMPONENT}_INCLUDE_DIR
        NAMES ${_TBB_${_TBB_COMPONENT}_INC_NAMES}
        HINTS ${TBB_INCLUDE_DIR} ${TBB_ROOT}
        PATH_SUFFIXES ${_TBB_INC_PATH_SUFFIXES}
    )

    find_library(TBB_${_TBB_COMPONENT}_LIBRARY_RELEASE
        NAMES ${_TBB_${_TBB_COMPONENT}_LIB_NAMES_RELEASE}
        HINTS ${TBB_ROOT}
        PATH_SUFFIXES ${_TBB_LIB_PATH_SUFFIXES}
    )

    find_library(TBB_${_TBB_COMPONENT}_LIBRARY_DEBUG
        NAMES ${_TBB_${_TBB_COMPONENT}_LIB_NAMES_DEBUG}
        HINTS ${TBB_ROOT}
        PATH_SUFFIXES ${_TBB_LIB_PATH_SUFFIXES}
    )

    if(TBB_DEBUG)
        message("** FindTBB: - TBB_${_TBB_COMPONENT}_INCLUDE_DIR     = ${TBB_${_TBB_COMPONENT}_INCLUDE_DIR}")
        message("** FindTBB: - TBB_${_TBB_COMPONENT}_LIBRARY_RELEASE = ${TBB_${_TBB_COMPONENT}_LIBRARY_RELEASE}")
        message("** FindTBB: - TBB_${_TBB_COMPONENT}_LIBRARY_DEBUG   = ${TBB_${_TBB_COMPONENT}_LIBRARY_DEBUG}")
    endif()

    # Mark cache entries as advanced
    mark_as_advanced(TBB_${_TBB_COMPONENT}_INCLUDE_DIR)
    mark_as_advanced(TBB_${_TBB_COMPONENT}_LIBRARY_RELEASE)
    mark_as_advanced(TBB_${_TBB_COMPONENT}_LIBRARY_DEBUG)

    # Set TBB_<C>_LIBRARY
    if(TBB_${_TBB_COMPONENT}_LIBRARY_RELEASE AND TBB_${_TBB_COMPONENT}_LIBRARY_DEBUG)
        set(TBB_${_TBB_COMPONENT}_LIBRARY
            optimized ${TBB_${_TBB_COMPONENT}_LIBRARY_RELEASE}
            debug     ${TBB_${_TBB_COMPONENT}_LIBRARY_DEBUG}
        )
    elseif(TBB_${_TBB_COMPONENT}_LIBRARY_RELEASE)
        set(TBB_${_TBB_COMPONENT}_LIBRARY ${TBB_${_TBB_COMPONENT}_LIBRARY_RELEASE})
    elseif(TBB_${_TBB_COMPONENT}_LIBRARY_DEBUG)
        set(TBB_${_TBB_COMPONENT}_LIBRARY ${TBB_${_TBB_COMPONENT}_LIBRARY_DEBUG})
    else()
        set(TBB_${_TBB_COMPONENT}_LIBRARY TBB_${_TBB_COMPONENT}_LIBRARY-NOTFOUND)
    endif()

    # Set TBB_<C>_FOUND
    if(TBB_${_TBB_COMPONENT}_INCLUDE_DIR AND TBB_${_TBB_COMPONENT}_LIBRARY)
        set(TBB_${_TBB_COMPONENT}_FOUND TRUE)
    else()
        set(TBB_${_TBB_COMPONENT}_FOUND FALSE)
    endif()
    set(TBB_${_TBB_${_TBB_COMPONENT}_NAME}_FOUND ${TBB_${_TBB_COMPONENT}_FOUND})

    if(TBB_${_TBB_COMPONENT}_FOUND)
        # Add transitive dependencies
        set(TBB_${_TBB_COMPONENT}_INCLUDE_DIRS ${TBB_${_TBB_COMPONENT}_INCLUDE_DIR})
        set(TBB_${_TBB_COMPONENT}_LIBRARIES    ${TBB_${_TBB_COMPONENT}_LIBRARY})
        if(_TBB_${_TBB_COMPONENT}_LIB_LINK_DEPENDS)
            list(APPEND TBB_${_TBB_COMPONENT}_LIBRARIES "${_TBB_${_TBB_COMPONENT}_LIB_LINK_DEPENDS}")
        endif()

        if(TBB_DEBUG)
            message("** FindTBB: - TBB_${_TBB_COMPONENT}_INCLUDE_DIRS    = [${TBB_${_TBB_COMPONENT}_INCLUDE_DIRS}]")
            message("** FindTBB: - TBB_${_TBB_COMPONENT}_LIBRARIES       = [${TBB_${_TBB_COMPONENT}_LIBRARIES}]")
        endif()

        # Add to TBB_INCLUDE_DIRS and TBB_LIBRARIES
        list(APPEND TBB_INCLUDE_DIRS ${TBB_${_TBB_COMPONENT}_INCLUDE_DIRS})
        list(APPEND TBB_LIBRARIES    ${TBB_${_TBB_COMPONENT}_LIBRARIES})

        # Add TBB::<C> import target
        string(TOLOWER ${_TBB_COMPONENT} _TBB_TARGET_NAME)
        set(_TBB_TARGET_NAME "TBB::${_TBB_TARGET_NAME}")
        add_library(${_TBB_TARGET_NAME} SHARED IMPORTED)

        set_target_properties(${_TBB_TARGET_NAME} PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES     "${TBB_${_TBB_COMPONENT}_INCLUDE_DIRS}"
            IMPORTED_LINK_INTERFACE_LANGUAGES CXX
            IMPORTED_NO_SONAME                TRUE
        )
        if(_TBB_${_TBB_COMPONENT}_LIB_LINK_DEPENDS)
            set_target_properties(${_TBB_TARGET_NAME} PROPERTIES
            INTERFACE_LINK_LIBRARIES "${_TBB_${_TBB_COMPONENT}_LIB_LINK_DEPENDS}"
        )
        endif()

        foreach(_TBB_CONFIGURATION IN ITEMS DEBUG RELEASE)
            if(TBB_${_TBB_COMPONENT}_LIBRARY_${_TBB_CONFIGURATION})
                set_property(TARGET ${_TBB_TARGET_NAME} APPEND PROPERTY IMPORTED_CONFIGURATIONS ${_TBB_CONFIGURATION})
            if(WIN32)
                set_target_properties(${_TBB_TARGET_NAME} PROPERTIES
                    IMPORTED_IMPLIB_${_TBB_CONFIGURATION} "${TBB_${_TBB_COMPONENT}_LIBRARY_${_TBB_CONFIGURATION}}"
                )
                string(REPLACE       "/lib/"   "/bin/" _TBB_LIB_PATH_DLL "${TBB_${_TBB_COMPONENT}_LIBRARY_${_TBB_CONFIGURATION}}")
                string(REGEX REPLACE "\\.lib$" ".dll"  _TBB_LIB_PATH_DLL "${_TBB_LIB_PATH_DLL}")
                if(EXISTS "${_TBB_LIB_PATH_DLL}")
                    set_target_properties(${_TBB_TARGET_NAME} PROPERTIES
                        IMPORTED_LOCATION_${_TBB_CONFIGURATION} "${_TBB_LIB_PATH_DLL}"
                    )
                if(TBB_DEBUG)
                    message("** FindTBB: - IMPORTED_LOCATION_${_TBB_CONFIGURATION} = ${_TBB_LIB_PATH_DLL}")
                endif()
                elseif(TBB_DEBUG)
                    message("** FindTBB: Could not determine ${_TBB_CONFIGURATION} DLL path from import library, tried: "
                        "\n\t${_TBB_LIB_PATH_DLL}")
                endif()
            else()
                set_target_properties(${_TBB_TARGET_NAME} PROPERTIES
                    IMPORTED_LOCATION_${_TBB_CONFIGURATION} "${TBB_${_TBB_COMPONENT}_LIBRARY_${_TBB_CONFIGURATION}}"
                )
            endif()
        endif()
    endforeach ()

    if(TBB_DEBUG)
        message("** FindTBB: Looking for component ${_TBB_COMPONENT}... - found")
    endif()

    else()
        if(TBB_DEBUG)
            message("** FindTBB: Looking for component ${_TBB_COMPONENT}... - not found")
        endif()
        unset(TBB_${_TBB_COMPONENT}_INCLUDE_DIRS)
        unset(TBB_${_TBB_COMPONENT}_LIBRARIES)
    endif()
endforeach ()

if(TBB_INCLUDE_DIRS)
    list(REMOVE_DUPLICATES TBB_INCLUDE_DIRS)
endif()

if(TBB_DEBUG)
    message("** FindTBB: Include paths and libraries of all found components:")
    message("** FindTBB: - TBB_INCLUDE_DIRS = [${TBB_INCLUDE_DIRS}]")
    message("** FindTBB: - TBB_LIBRARIES    = [${TBB_LIBRARIES}]")
endif()

# Extract library version from start of tbb_stddef.h
if(TBB_INCLUDE_DIR)
    if(NOT DEFINED TBB_VERSION_MAJOR OR
        NOT DEFINED TBB_VERSION_MINOR OR
        NOT DEFINED TBB_INTERFACE_VERSION OR
        NOT DEFINED TBB_COMPATIBLE_INTERFACE_VERSION)
        file(READ "${TBB_INCLUDE_DIR}/tbb/tbb_stddef.h" _TBB_VERSION_CONTENTS LIMIT 2048)
        string(REGEX REPLACE
            ".*#define TBB_VERSION_MAJOR ([0-9]+).*" "\\1"
            TBB_VERSION_MAJOR "${_TBB_VERSION_CONTENTS}"
        )
        string(REGEX REPLACE
            ".*#define TBB_VERSION_MINOR ([0-9]+).*" "\\1"
            TBB_VERSION_MINOR "${_TBB_VERSION_CONTENTS}"
        )
        string(REGEX REPLACE
            ".*#define TBB_INTERFACE_VERSION ([0-9]+).*" "\\1"
            TBB_INTERFACE_VERSION "${_TBB_VERSION_CONTENTS}"
        )
        string(REGEX REPLACE
            ".*#define TBB_COMPATIBLE_INTERFACE_VERSION ([0-9]+).*" "\\1"
            TBB_COMPATIBLE_INTERFACE_VERSION "${_TBB_VERSION_CONTENTS}"
        )
        unset(_TBB_VERSION_CONTENTS)
    endif()
    set(TBB_VERSION "${TBB_VERSION_MAJOR}.${TBB_VERSION_MINOR}")
    set(TBB_VERSION_STRING "${TBB_VERSION}")
    else()
        unset(TBB_VERSION)
        unset(TBB_VERSION_MAJOR)
        unset(TBB_VERSION_MINOR)
        unset(TBB_VERSION_STRING)
        unset(TBB_INTERFACE_VERSION)
        unset(TBB_COMPATIBLE_INTERFACE_VERSION)
    endif()

if(TBB_DEBUG)
    message("** FindTBB: Version information from ${TBB_INCLUDE_DIR}/tbb/tbb_stddef.h")
    message("** FindTBB: - TBB_VERSION_STRING               = ${TBB_VERSION_STRING}")
    message("** FindTBB: - TBB_VERSION_MAJOR                = ${TBB_VERSION_MAJOR}")
    message("** FindTBB: - TBB_VERSION_MINOR                = ${TBB_VERSION_MINOR}")
    message("** FindTBB: - TBB_INTERFACE_VERSION            = ${TBB_INTERFACE_VERSION}")
    message("** FindTBB: - TBB_COMPATIBLE_INTERFACE_VERSION = ${TBB_COMPATIBLE_INTERFACE_VERSION}")
endif()

# Handle QUIET, REQUIRED, and [EXACT] VERSION arguments and set TBB_FOUND
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(TBB
    REQUIRED_VARS TBB_INCLUDE_DIR
    VERSION_VAR   TBB_VERSION
    HANDLE_COMPONENTS
)

if(NOT TBB_FIND_QUIETLY)
    if(TBB_FOUND)
        message(STATUS "${_TBB_FIND_STATUS}... - found v${TBB_VERSION_STRING}")
    else()
        message(STATUS "${_TBB_FIND_STATUS}... - not found")
    endif()
endif()

# Unset local auxiliary variables
foreach(_TBB_COMPONENT IN ITEMS TBB MALLOC MALLOC_PROXY)
    unset(_TBB_FIND_REQUIRED_${_TBB_COMPONENT})
    unset(_TBB_${_TBB_COMPONENT}_LIB_NAMES_RELEASE)
    unset(_TBB_${_TBB_COMPONENT}_LIB_NAMES_DEBUG)
    unset(_TBB_${_TBB_COMPONENT}_LIB_LINK_DEPENDS)
    unset(_TBB_${_TBB_COMPONENT}_INC_NAMES)
    unset(_TBB_${_TBB_COMPONENT}_NAME)
endforeach()

unset(_TBB_COMPONENT)
unset(_TBB_TARGET_NAME)
unset(_TBB_FIND_COMPONENTS)
unset(_TBB_FIND_STATUS)
unset(_TBB_INC_PATH_SUFFIXES)
unset(_TBB_LIB_PATH_SUFFIXES)
unset(_TBB_LIB_PATH_DLL)
unset(_TBB_LIB_NAME)
unset(_TBB_ARCH_PLATFORM)
