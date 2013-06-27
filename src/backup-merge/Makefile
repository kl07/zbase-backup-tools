CC = g++ -ljemalloc
FLAGS = -lsqlite3
SPARSEHASH_FLAGS = -I google-sparsehash

all: CC += -O3
all: main

debug: CC += -DSHOW_TIME -g
debug: main

main: sqlite-objects.o util.o backup.o main.o
	$(CC) $(FLAGS) main.o sqlite-objects.o util.o backup.o -o merge-incremental

main.o: main.cc timing.hh
	$(CC) -c main.cc $(SPARSEHASH_FLAGS)

sqlite-objects.o: sqlite-objects.cc sqlite-objects.hh
	$(CC) -c sqlite-objects.cc

util.o: util.hh util.cc
	$(CC) -c util.cc

backup.o: backup.hh backup.cc
	$(CC) -c backup.cc $(SPARSEHASH_FLAGS)

clean:
	rm -f *.o merge-incremental
