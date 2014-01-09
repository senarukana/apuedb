
CFLAGS= -Wall -ggdb

AR = ar
STATIC_LIB = libapue_db.a
DYNAMIC_LIB = libapue_db.so.1

COM_OBJ = apue_db.o error.o lock.o
OBJ = apue_db.c error.c lock.c

all: ${STATIC_LIB} libapue_db.so.1 test

lock.o: error.h

apue_db.o: error.h lock.h apue_db.h

libapue_db.a: ${COM_OBJ}
	$(AR) rv ${STATIC_LIB} ${COM_OBJ}

libapue_db.so.1: ${OBJ}
	gcc -fPIC -g -c -Wall error.c
	gcc -fPIC -g -c -Wall lock.c
	gcc -fPIC -g -c -Wall apue_db.c
	gcc -shared -Wl,-soname,${DYNAMIC_LIB} -o ${DYNAMIC_LIB} ${COM_OBJ} 

test.o: error.h apue_db.h

test: ${STATIC_LIB} test.o
	gcc ${CFLAGS} -o test test.o ${STATIC_LIB}

clean:
	rm -f *.o core test $(STATIC_LIB) ${DYNAMIC_LIB}
 