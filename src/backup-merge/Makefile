FLAGS = -lsqlite3 -ljemalloc -std=gnu++0x -W -O3

SOURCE = merge-incremental.cc
CC = g++
TARGET = merge-incremental
INCLUDE = google-sparsehash

default:
	$(CC) $(SOURCE) $(FLAGS) -o $(TARGET) -I $(INCLUDE)

debug:
	$(CC) $(SOURCE) $(FLAGS) -o $(TARGET) -DSHOW_TIME -I $(INCLUDE)

clean:
	rm $(TARGET)
