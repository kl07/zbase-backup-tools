FLAGS = -lsqlite3 -std=gnu++0x -W -O3

SOURCE = merge-incremental.cc
CC = g++
TARGET = merge-incremental

default:
	$(CC) $(SOURCE) $(FLAGS) -o $(TARGET)

debug:
	$(CC) $(SOURCE) $(FLAGS) -o $(TARGET) -DSHOW_TIME

clean:
	rm $(TARGET)
