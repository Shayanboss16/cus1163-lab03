#include "process_manager.h"

static void producer(int write_fd, int start, int end) {
    printf("Producer (PID: %d) starting...\n", getpid());
    fflush(stdout);

    for (int n = start; n <= end; n++) {
        if (write(write_fd, &n, sizeof(n)) != (ssize_t)sizeof(n)) {
            perror("producer write");
            _exit(1);
        }
        printf("Producer: Sent number %d\n", n);
        fflush(stdout);
        usleep(80000);
    }

    printf("Producer: Finished sending %d numbers\n", end - start + 1);
    fflush(stdout);
    close(write_fd);
    _exit(0);
}

static void consumer(int read_fd) {
    printf("Consumer (PID: %d) starting...\n", getpid());
    fflush(stdout);

    int sum = 0, x = 0;
    ssize_t nread;
    while ((nread = read(read_fd, &x, sizeof(x))) > 0) {
        if (nread != (ssize_t)sizeof(x)) {
            fprintf(stderr, "Consumer: partial read\n");
            _exit(2);
        }
        sum += x;
        printf("Consumer: Received %d, running sum: %d\n", x, sum);
        fflush(stdout);
    }
    if (nread < 0) {
        perror("consumer read");
        _exit(3);
    }

    printf("Consumer: Final sum: %d\n", sum);
    fflush(stdout);
    close(read_fd);
    _exit(0);
}

int run_basic_demo(void) {
    int pipe_fd[2];
    if (pipe(pipe_fd) == -1) {
        perror("pipe");
        return -1;
    }

    printf("\nStarting basic producer-consumer demonstration...\n\n");
    printf("Parent process (PID: %d) creating children...\n", getpid());
    fflush(stdout);

    pid_t prod = fork();
    if (prod < 0) {
        perror("fork (producer)");
        close(pipe_fd[0]); close(pipe_fd[1]);
        return -1;
    }
    if (prod == 0) {
        close(pipe_fd[0]);
        producer(pipe_fd[1], 1, 5);
    }
    printf("Created producer child (PID: %d)\n", prod);

    pid_t cons = fork();
    if (cons < 0) {
        perror("fork (consumer)");
        close(pipe_fd[0]); close(pipe_fd[1]);
        int st; waitpid(prod, &st, 0);
        return -1;
    }
    if (cons == 0) {
        close(pipe_fd[1]);
        consumer(pipe_fd[0]);
    }
    printf("Created consumer child (PID: %d)\n\n", cons);

    close(pipe_fd[0]); close(pipe_fd[1]);

    int st1 = 0, st2 = 0;
    pid_t w1 = waitpid(prod, &st1, 0);
    if (w1 == -1) {
        perror("waitpid producer");
    } else {
        int code = WIFEXITED(st1) ? WEXITSTATUS(st1)
                  : (WIFSIGNALED(st1) ? 128 + WTERMSIG(st1) : -1);
        printf("\nProducer child (PID: %d) exited with status %d\n", w1, code);
    }

    pid_t w2 = waitpid(cons, &st2, 0);
    if (w2 == -1) {
        perror("waitpid consumer");
    } else {
        int code = WIFEXITED(st2) ? WEXITSTATUS(st2)
                  : (WIFSIGNALED(st2) ? 128 + WTERMSIG(st2) : -1);
        printf("Consumer child (PID: %d) exited with status %d\n", w2, code);
    }

    printf("\nSUCCESS: Basic producer-consumer completed!\n");
    fflush(stdout);
    return 0;
}

int run_multiple_pairs(int num_pairs) {
    if (num_pairs <= 0) {
        fprintf(stderr, "num_pairs must be >= 1\n");
        return -1;
    }

    printf("\nRunning multiple producer-consumer pairs...\n\n");
    printf("Parent creating %d producer-consumer pairs...\n\n", num_pairs);
    fflush(stdout);

    pid_t *kids = (pid_t*)calloc(num_pairs * 2, sizeof(pid_t));
    if (!kids) {
        perror("calloc");
        return -1;
    }
    int kid_count = 0;

    int start = 1;
    for (int pair = 1; pair <= num_pairs; pair++) {
        int end = start + 5 - 1;
        printf("=== Pair %d ===\n", pair);
        fflush(stdout);

        int fds[2];
        if (pipe(fds) == -1) {
            perror("pipe");
            break;
        }

        pid_t p = fork();
        if (p < 0) {
            perror("fork (producer)");
            close(fds[0]); close(fds[1]);
            break;
        }
        if (p == 0) {
            close(fds[0]);
            producer(fds[1], start, end);
        }
        kids[kid_count++] = p;

        pid_t c = fork();
        if (c < 0) {
            perror("fork (consumer)");
            close(fds[0]); close(fds[1]);
            break;
        }
        if (c == 0) {
            close(fds[1]);
            consumer(fds[0]);
        }
        kids[kid_count++] = c;

        close(fds[0]); close(fds[1]);

        start = end + 1;
    }

    printf("\nAll pairs created. Waiting for children...\n");
    fflush(stdout);

    for (int i = 0; i < kid_count; i++) {
        int st = 0;
        pid_t w = waitpid(kids[i], &st, 0);
        if (w == -1) {
            perror("waitpid");
            continue;
        }
        int code = WIFEXITED(st) ? WEXITSTATUS(st)
                  : (WIFSIGNALED(st) ? 128 + WTERMSIG(st) : -1);
        printf("Child (PID: %d) exited with status %d\n", w, code);
        fflush(stdout);
    }

    free(kids);
    printf("\nSUCCESS: Multiple pairs completed!\n");
    fflush(stdout);
    return 0;
}

