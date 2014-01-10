#include "apue_db.h"
#include "error.h"
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h> // tolower

/*
 * Default file access permissions for new files.
 */
#define	FILE_MODE	(S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)

#define COMMAND_SEP ' '

int MAX_KEYLEN = 1024;
int MAX_VALEN = 1024;
int MAX_LEN = 2048;



void to_lowercase(char *str) {
	int i;
	int len = strlen(str);

	for (i = 0; i < len; i++) {
		str[i] = tolower(str[i]);
	}
}

// char *strndup(char *str, size_t n) {
// 	char *s;
// 	if ((s = malloc(n+1)) == NULL) {
// 		return 0;
// 	}
// 	s[n] = '\0';
// 	return (char *)memcpy(s, str, n);
// }

int main(int argc, char **argv) {
	DBHANDLE db;
	int rc,c, flags, len;
	char *ptr_command, *ptr_key, *find;
	char *command, *key, *value;
	command = key = value = NULL;
	char *help_cmd = "command list:\n [add | update] key value \n [del | find] key \n [open | close] dbname\n [help count iterate exit]";

	flags = O_RDWR;
	while ((c = getopt(argc, argv, "nt?")) != -1) {
		switch (c) {
		case 'n':
			flags |= O_CREAT;
			break;
		case 't':
			flags |= O_TRUNC;
			break;
		case '?':
			err_quit("usage: [-n] create new file [-t] truncate db <filename>");
			break;
		}
	}
	if (optind != argc - 1) {
		err_quit("usage: [-n] create new file [-t] truncate db <filename>");
	}

	if ((db = db_open(argv[optind], flags, FILE_MODE)) == NULL) {
		err_sys("db_open %s error", argv[optind]);
	}

	for (;;) {
		char buf[MAX_LEN];
		printf("APUEDB> ");
		if (fgets(buf, MAX_LEN, stdin) == NULL) {
			printf("Bye~\n");
			exit(1);
		}
		to_lowercase(buf);
		len = strlen(buf);
		if ((ptr_command = strchr(buf,COMMAND_SEP)) == NULL) {
			buf[len-1] = '\0';
			if (strcmp(buf,"help") == 0) {
				printf("%s\n", help_cmd);
				continue;
			} else if (strcmp(buf, "count") == 0) {
				printf("%d\n", db_count(db));
			} else if (strcmp(buf, "iterate") == 0) {
				char *ptr;
				db_rewind(db);
				while ((ptr = db_nextrec(db, NULL)) != NULL) {
					printf("%s\n", ptr);
				}
				continue;
			} else if (strcmp(buf, "exit") == 0) {
				printf("Bye~\n");
				exit(1);
			} else {
				printf("invalid command, use command help for help\n");
				continue;
			}
		}
		command = strndup(buf, ptr_command - buf);

		if (strcmp(command, "add") == 0) {
			if ((ptr_key = strchr(ptr_command+1, COMMAND_SEP)) == NULL) {
				printf("invalid add command, usage: add key value\n");
			} else {
				key = strndup(ptr_command+1, ptr_key - ptr_command - 1);
				value = strndup(ptr_key+1, buf + len - 1 - ptr_key - 1);
				rc = db_store(db, key, value, DB_INSERT);
				if (rc == 0) {
					printf("add +OK\n");
				} else {
					printf("key %s already exist\n", key);
				}
			}
		} else if (strcmp(command, "update") == 0) {
			if ((ptr_key = strchr(ptr_command+1, COMMAND_SEP)) == NULL) {
				printf("invalid add command, usage: add key value\n");
			} else {
				key = strndup(ptr_command+1, ptr_key - ptr_command - 1);
				value = strndup(ptr_key+1, buf + len - 1 - ptr_key - 1);
				rc = db_store(db, key, value, DB_REPLACE);
				if (rc == 0) {
					printf("update +OK\n");
				} else {
					printf("key %s not found\n", key);
				}
			}
		} else if (strcmp(command, "del") == 0) {
			if ((ptr_key = strchr(ptr_command+1, COMMAND_SEP)) != NULL) {
				printf("invalid del command, usage: del key\n");
				continue;
			}
			key = strndup(ptr_command+1, buf + len - 1 - ptr_command -1);
			rc = db_delete(db, key);
			if (rc == 0) {
				printf("del +OK\n");
			} else {
				printf("key %s not found\n", key);
			}
		} else if (strcmp(command, "find") == 0) {
			if ((ptr_key = strchr(ptr_command+1, COMMAND_SEP)) != NULL) {
				printf("invalid find command, usage: find key\n");
				continue;
			}
			key = strndup(ptr_command+1, buf + len - 1 - ptr_command -1);
			find = db_fetch(db, key);
			if (find != NULL) {
				printf("%d,%s\n", strlen(find), find);
			} else {
				printf("key %s not found\n", key);
			}
		}
		if (key != NULL) {
			free(key);
			key = NULL;
		}
		if (command != NULL) {
			free(command);
			command = NULL;
		}
		if (value != NULL) {
			free(value);
			value = NULL;
		}
	}
}