##
## Auto Generated makefile by CodeLite IDE
## any manual changes will be erased      
##
## Debug
ProjectName            :=amqpConnPool
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
ObjectsFileList        :="amqpConnPool.txt"
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
Objects0=$(IntermediateDirectory)/src_amqp_conn_pool.cpp$(ObjectSuffix) 



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
	@echo rebuilt > "/home/abhimanyu/Documents/uwsgi_plugin/.build-debug/amqpConnPool"

MakeIntermediateDirs:
	@test -d ./Debug || $(MakeDirCommand) ./Debug


./Debug:
	@test -d ./Debug || $(MakeDirCommand) ./Debug

PreBuild:


##
## Objects
##
$(IntermediateDirectory)/src_amqp_conn_pool.cpp$(ObjectSuffix): src/amqp_conn_pool.cpp $(IntermediateDirectory)/src_amqp_conn_pool.cpp$(DependSuffix)
	$(CXX) $(IncludePCH) $(SourceSwitch) "/home/abhimanyu/Documents/uwsgi_plugin/src/amqp_conn_pool.cpp" $(CXXFLAGS) $(ObjectSwitch)$(IntermediateDirectory)/src_amqp_conn_pool.cpp$(ObjectSuffix) $(IncludePath)
$(IntermediateDirectory)/src_amqp_conn_pool.cpp$(DependSuffix): src/amqp_conn_pool.cpp
	@$(CXX) $(CXXFLAGS) $(IncludePCH) $(IncludePath) -MG -MP -MT$(IntermediateDirectory)/src_amqp_conn_pool.cpp$(ObjectSuffix) -MF$(IntermediateDirectory)/src_amqp_conn_pool.cpp$(DependSuffix) -MM "src/amqp_conn_pool.cpp"

$(IntermediateDirectory)/src_amqp_conn_pool.cpp$(PreprocessSuffix): src/amqp_conn_pool.cpp
	$(CXX) $(CXXFLAGS) $(IncludePCH) $(IncludePath) $(PreprocessOnlySwitch) $(OutputSwitch) $(IntermediateDirectory)/src_amqp_conn_pool.cpp$(PreprocessSuffix) "src/amqp_conn_pool.cpp"


-include $(IntermediateDirectory)/*$(DependSuffix)
##
## Clean
##
clean:
	$(RM) -r ./Debug/


