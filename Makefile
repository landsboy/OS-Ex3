all: ex3.out

ex3.out: mymain.c
	gcc mymain.c -o ex3.out -pthread -lrt

clean:
	rm -f ex3.out
