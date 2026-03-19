# Variables
CXX = g++
CXXFLAGS = -std=c++17 -static
LIBS = -lstdc++ -lstdc++fs
TARGET = db_app.exe
SRC = main.cpp

# Declaring phony targets
.PHONY: all run clean

# Default target
all: $(TARGET)

# Compile target
$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) $(SRC) -o $(TARGET) $(LIBS)

# Run target (it depends on the TARGET being built first)
run: $(TARGET)
	./$(TARGET)

# Clean target
clean:
	@if exist $(TARGET) del $(TARGET)