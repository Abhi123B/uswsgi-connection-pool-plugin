##
## Auto Generated makefile by CodeLite IDE
## any manual changes will be erased      
##
## Debug
ProjectName            :=commonUtils
ConfigurationName      :=Debug
WorkspacePath          := "/home/abhimanyu/Documents/uwsgi_plugin"
ProjectPath            := "/home/abhimanyu/Documents/uwsgi_plugin"
IntermediateDirectory  :=./Debug
OutDir                 := $(IntermediateDirectory)
CurrentFileName        :=
CurrentFilePath        :=
CurrentFileFullPath    :=
User                   :=
Date                   :=21/02/17
CodeLitePath           :="/home/abhimanyu/.codelite"
LinkerName             :=/usr/bin/g++
SharedObjectLinkerName :=/usr/bin/g++ -shared -fPIC
ObjectSuffix           :=.o
DependSuffix           :=.o.d
PreprocessSuffix       :=.i
DebugSwitch            :=-g 
IncludeSwitch          :=-I
LibrarySwitch          :=-l
OutputSwitch           :=-o 
LibraryPathSwitch      :=-L
PreprocessorSwitch     :=-D
SourceSwitch           :=-c 
OutputFile             :=./libs/lib$(ProjectName).a
Preprocessors          :=
ObjectSwitch           :=-o 
ArchiveOutputSwitch    := 
PreprocessOnlySwitch   :=-E
ObjectsFileList        :="commonUtils.txt"
PCHCompileFlags        :=
MakeDirCommand         :=mkdir -p
LinkOptions            :=  
IncludePath            :=  $(IncludeSwitch). $(IncludeSwitch). 
IncludePCH             := 
RcIncludePath          := 
Libs                   := 
ArLibs                 :=  
LibPath                := $(LibraryPathSwitch). 

##
## Common variables
## AR, CXX, CC, AS, CXXFLAGS and CFLAGS can be overriden using an environment variables
##
AR       := /usr/bin/ar rcu
CXX      := /usr/bin/g++
CC       := /usr/bin/gcc
CXXFLAGS :=  -g -fPIC $(Preprocessors)
CFLAGS   :=  -g -fPIC $(Preprocessors)
ASFLAGS  := 
AS       := /usr/bin/as


##
## User defined environment variables
##
CodeLiteDir:=/usr/share/codelite
Objects0=$(IntermediateDirectory)/src_error_defs.cpp$(ObjectSuffix) $(IntermediateDirectory)/src_parser_defs.cpp$(ObjectSuffix) $(IntermediateDirectory)/src_db_helper.cpp$(ObjectSuffix) $(IntermediateDirectory)/src_file_helper.cpp$(ObjectSuffix) 



Objects=$(Objects0) 

##
## Main Build Targets 
##
.PHONY: all clean PreBuild PrePreBuild PostBuild MakeIntermediateDirs
all: $(IntermediateDirectory) $(OutputFile)

$(OutputFile): $(Objects)
	@$(MakeDirCommand) $(@D)
	@echo "" > $(IntermediateDirectory)/.d
	@echo $(Objects0)  > $(ObjectsFileList)
	$(AR) $(ArchiveOutputSwitch)$(OutputFile) @$(ObjectsFileList) $(ArLibs)
	@$(MakeDirCommand) "/home/abhimanyu/Documents/uwsgi_plugin/.build-debug"
	@echo rebuilt > "/home/abhimanyu/Documents/uwsgi_plugin/.build-debug/commonUtils"

MakeIntermediateDirs:
	@test -d ./Debug || $(MakeDirCommand) ./Debug


./Debug:
	@test -d ./Debug || $(MakeDirCommand) ./Debug

PreBuild:


##
## Objects
##
$(IntermediateDirectory)/src_error_defs.cpp$(ObjectSuffix): src/error_defs.cpp $(IntermediateDirectory)/src_error_defs.cpp$(DependSuffix)
	$(CXX) $(IncludePCH) $(SourceSwitch) "/home/abhimanyu/Documents/uwsgi_plugin/src/error_defs.cpp" $(CXXFLAGS) $(ObjectSwitch)$(IntermediateDirectory)/src_error_defs.cpp$(ObjectSuffix) $(IncludePath)
$(IntermediateDirectory)/src_error_defs.cpp$(DependSuffix): src/error_defs.cpp
	@$(CXX) $(CXXFLAGS) $(IncludePCH) $(IncludePath) -MG -MP -MT$(IntermediateDirectory)/src_error_defs.cpp$(ObjectSuffix) -MF$(IntermediateDirectory)/src_error_defs.cpp$(DependSuffix) -MM "src/error_defs.cpp"

$(IntermediateDirectory)/src_error_defs.cpp$(PreprocessSuffix): src/error_defs.cpp
	$(CXX) $(CXXFLAGS) $(IncludePCH) $(IncludePath) $(PreprocessOnlySwitch) $(OutputSwitch) $(IntermediateDirectory)/src_error_defs.cpp$(PreprocessSuffix) "src/error_defs.cpp"

$(IntermediateDirectory)/src_parser_defs.cpp$(ObjectSuffix): src/parser_defs.cpp $(IntermediateDirectory)/src_parser_defs.cpp$(DependSuffix)
	$(CXX) $(IncludePCH) $(SourceSwitch) "/home/abhimanyu/Documents/uwsgi_plugin/src/parser_defs.cpp" $(CXXFLAGS) $(ObjectSwitch)$(IntermediateDirectory)/src_parser_defs.cpp$(ObjectSuffix) $(IncludePath)
$(IntermediateDirectory)/src_parser_defs.cpp$(DependSuffix): src/parser_defs.cpp
	@$(CXX) $(CXXFLAGS) $(IncludePCH) $(IncludePath) -MG -MP -MT$(IntermediateDirectory)/src_parser_defs.cpp$(ObjectSuffix) -MF$(IntermediateDirectory)/src_parser_defs.cpp$(DependSuffix) -MM "src/parser_defs.cpp"

$(IntermediateDirectory)/src_parser_defs.cpp$(PreprocessSuffix): src/parser_defs.cpp
	$(CXX) $(CXXFLAGS) $(IncludePCH) $(IncludePath) $(PreprocessOnlySwitch) $(OutputSwitch) $(IntermediateDirectory)/src_parser_defs.cpp$(PreprocessSuffix) "src/parser_defs.cpp"

$(IntermediateDirectory)/src_db_helper.cpp$(ObjectSuffix): src/db_helper.cpp $(IntermediateDirectory)/src_db_helper.cpp$(DependSuffix)
	$(CXX) $(IncludePCH) $(SourceSwitch) "/home/abhimanyu/Documents/uwsgi_plugin/src/db_helper.cpp" $(CXXFLAGS) $(ObjectSwitch)$(IntermediateDirectory)/src_db_helper.cpp$(ObjectSuffix) $(IncludePath)
$(IntermediateDirectory)/src_db_helper.cpp$(DependSuffix): src/db_helper.cpp
	@$(CXX) $(CXXFLAGS) $(IncludePCH) $(IncludePath) -MG -MP -MT$(IntermediateDirectory)/src_db_helper.cpp$(ObjectSuffix) -MF$(IntermediateDirectory)/src_db_helper.cpp$(DependSuffix) -MM "src/db_helper.cpp"

$(IntermediateDirectory)/src_db_helper.cpp$(PreprocessSuffix): src/db_helper.cpp
	$(CXX) $(CXXFLAGS) $(IncludePCH) $(IncludePath) $(PreprocessOnlySwitch) $(OutputSwitch) $(IntermediateDirectory)/src_db_helper.cpp$(PreprocessSuffix) "src/db_helper.cpp"

$(IntermediateDirectory)/src_file_helper.cpp$(ObjectSuffix): src/file_helper.cpp $(IntermediateDirectory)/src_file_helper.cpp$(DependSuffix)
	$(CXX) $(IncludePCH) $(SourceSwitch) "/home/abhimanyu/Documents/uwsgi_plugin/src/file_helper.cpp" $(CXXFLAGS) $(ObjectSwitch)$(IntermediateDirectory)/src_file_helper.cpp$(ObjectSuffix) $(IncludePath)
$(IntermediateDirectory)/src_file_helper.cpp$(DependSuffix): src/file_helper.cpp
	@$(CXX) $(CXXFLAGS) $(IncludePCH) $(IncludePath) -MG -MP -MT$(IntermediateDirectory)/src_file_helper.cpp$(ObjectSuffix) -MF$(IntermediateDirectory)/src_file_helper.cpp$(DependSuffix) -MM "src/file_helper.cpp"

$(IntermediateDirectory)/src_file_helper.cpp$(PreprocessSuffix): src/file_helper.cpp
	$(CXX) $(CXXFLAGS) $(IncludePCH) $(IncludePath) $(PreprocessOnlySwitch) $(OutputSwitch) $(IntermediateDirectory)/src_file_helper.cpp$(PreprocessSuffix) "src/file_helper.cpp"


-include $(IntermediateDirectory)/*$(DependSuffix)
##
## Clean
##
clean:
	$(RM) -r ./Debug/


