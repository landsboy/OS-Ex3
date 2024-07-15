// Netanel Landesman
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <string.h>

typedef struct {
    int producer_id;
    int type;
    int production_number;
} NewsItem;

typedef struct {
    NewsItem *buffer;
    int size;
    int in;
    int out;
    sem_t empty;
    sem_t full;
    pthread_mutex_t mutex;
} BoundedQueue;

typedef struct {
    NewsItem item;
    struct Node *next;
} Node;

typedef struct {
    Node *head;
    Node *tail;
    sem_t items;
    pthread_mutex_t mutex;
} UnboundedQueue;

typedef struct {
    int id;
    int production_number;
    int queue_size;
    BoundedQueue *bounded_queue;
} Producer;

typedef struct {
    Producer *producers;
    int num_of_producers;
} ProducerList;

typedef struct {
    BoundedQueue *bounded_queue;
    UnboundedQueue *unbounded_queue;
} CoEditor;

typedef struct {
    CoEditor *coEditors;
    int num_of_coEditors;
} CoEditorsList;

typedef struct {
    int numProducers;
    BoundedQueue **producer_queue;
    UnboundedQueue **coEditor_queue;
} Dispatcher;

BoundedQueue initialize_bounded_queue(int size) {
    BoundedQueue bqueue;
    bqueue.size = size;
    bqueue.buffer = (NewsItem *) malloc(size * sizeof(NewsItem));
    bqueue.in = 0;
    bqueue.out = 0;
    sem_init(&bqueue.empty, 0, bqueue.size);
    sem_init(&bqueue.full, 0, 0);
    pthread_mutex_init(&bqueue.mutex, NULL);
    return bqueue;
}

void bounded_enqueue(BoundedQueue *bqueue, NewsItem *item) {
    sem_wait(&bqueue->empty);
    pthread_mutex_lock(&bqueue->mutex);
    bqueue->buffer[bqueue->in] = *item;
    bqueue->in = (bqueue->in + 1) % bqueue->size;
    pthread_mutex_unlock(&bqueue->mutex);
    sem_post(&bqueue->full);
}

NewsItem bounded_dequeue(BoundedQueue *bqueue) {
    sem_wait(&bqueue->full);
    pthread_mutex_lock(&bqueue->mutex);
    NewsItem *item = &bqueue->buffer[bqueue->out];
    bqueue->out = (bqueue->out + 1) % bqueue->size;
    pthread_mutex_unlock(&bqueue->mutex);
    sem_post(&bqueue->empty);
    return *item;
}

UnboundedQueue initialize_unbounded_queue() {
    UnboundedQueue ubqueue;
    ubqueue.head = NULL;
    ubqueue.tail = NULL;
    sem_init(&ubqueue.items, 0, 0);
    pthread_mutex_init(&ubqueue.mutex, NULL);
    return ubqueue;
}

void destroy_ubqueue(UnboundedQueue *buffer) {
    Node *current = buffer->head;
    while (current != NULL) {
        Node *next = (Node *) current->next;
        free(current);
        current = next;
    }
    sem_destroy(&buffer->items);
    pthread_mutex_destroy(&buffer->mutex);
}

void unbounded_enqueue(UnboundedQueue *ubqueue, NewsItem *item) {
    Node *newNode = (Node *) malloc(sizeof(Node));
    newNode->item = *item;
    newNode->next = NULL;

    pthread_mutex_lock(&ubqueue->mutex);

    if (ubqueue->head == NULL) {
        ubqueue->head = newNode;
        ubqueue->tail = newNode;
    } else {
        ubqueue->tail->next = (struct Node *) newNode;
        ubqueue->tail = newNode;
    }

    sem_post(&ubqueue->items);
    pthread_mutex_unlock(&ubqueue->mutex);
}

NewsItem unbounded_dequeue(UnboundedQueue *ubqueue) {
    sem_wait(&ubqueue->items);
    pthread_mutex_lock(&ubqueue->mutex);

    Node *nodeToRemove = ubqueue->head;
    NewsItem item = nodeToRemove->item;

    ubqueue->head = (Node *) ubqueue->head->next;
    free(nodeToRemove);

    pthread_mutex_unlock(&ubqueue->mutex);
    return item;
}

void readConfFile(ProducerList *producerList, char *confFile, int *boundq) {
    FILE *file = fopen(confFile, "r");
    if (file == NULL) {
        printf("Failed to open file: %s\n", confFile);
        exit(-1);
    }
    int numProducers = 0;

    char line[100];
    while (fgets(line, sizeof(line), file) != NULL) {
        char temp[100];
        strcpy(temp, line);
        if (fgets(line, sizeof(line), file) != NULL) {
            numProducers++;
            fgets(line, sizeof(line), file);
            fgets(line, sizeof(line), file);
        } else {
            *boundq = atoi(line);
        }
    }
    fseek(file, 0, SEEK_SET);
    producerList->num_of_producers = numProducers;
    producerList->producers = malloc(numProducers * sizeof(Producer));
    for (int i = 0; i < numProducers; ++i) {
        Producer producer;
        //producer id
        fgets(line, sizeof(line), file);
        producer.id = atoi(line);
        //number of products
        fgets(line, sizeof(line), file);
        producer.production_number = atoi(line);
        //size of queue
        fgets(line, sizeof(line), file);
        producer.queue_size = atoi(line);
        //new line
        fgets(line, sizeof(line), file);
        producerList->producers[i] = producer;
    }

    fclose(file);
}

void createBoundedQueues(ProducerList *producerList, BoundedQueue *bqueueList[], BoundedQueue *coEditorQueue,
                         const int *boundq) {
    //loop over the list of producers and create a bounded queue for each one
    for (int i = 0; i < producerList->num_of_producers; ++i) {
        BoundedQueue *bqueue = malloc(sizeof(BoundedQueue));
        if (bqueue == NULL) {
            printf("Error allocating memory for bounded queue\n");
            exit(-1);
        } else {
            //initialize the bounded queue
            *bqueue = initialize_bounded_queue(producerList->producers[i].queue_size);
            //add the bounded queue to its corresponding producer
            producerList->producers[i].bounded_queue = bqueue;
            //add the bounded queue to the list of bounded queues
            bqueueList[i] = malloc(sizeof(BoundedQueue));
            if (bqueueList[i] == NULL) {
                printf("Error allocating memory for bounded queue\n");
                exit(-1);
            } else {
                bqueueList[i] = bqueue;
            }
        }
    }
    //create bounded queue for co editor and screen manager
    *coEditorQueue = initialize_bounded_queue(*boundq);
}

void createCoEditors(CoEditorsList *coEditorsList, UnboundedQueue *unboundedQueueList[3], BoundedQueue *coEditorQueue) {
    coEditorsList->coEditors = malloc(sizeof(CoEditor) * coEditorsList->num_of_coEditors);
    for (int i = 0; i < 3; ++i) {
        UnboundedQueue *ubqueue = malloc(sizeof(UnboundedQueue));
        if (ubqueue == NULL) {
            printf("Error allocating memory for unbounded queue\n");
            exit(-1);
        } else {
            *ubqueue = initialize_unbounded_queue();
            CoEditor coEditor;
            coEditor.unbounded_queue = ubqueue;
            coEditor.bounded_queue = coEditorQueue;
            coEditorsList->coEditors[i] = coEditor;
            unboundedQueueList[i] = ubqueue;
        }

    }
}

void createDispatcher(Dispatcher *dispatcher, ProducerList *producerList, UnboundedQueue *unboundedQueueList[],
                      BoundedQueue *bqueueList[]) {
    dispatcher->numProducers = producerList->num_of_producers;
    dispatcher->coEditor_queue = malloc(3 * sizeof(UnboundedQueue));
    dispatcher->coEditor_queue = unboundedQueueList;

    //assign Producers queues to the dispatcher
    dispatcher->producer_queue = malloc(producerList->num_of_producers * sizeof(BoundedQueue));
    dispatcher->producer_queue = bqueueList;
}

void *producer(void *arg) {
    Producer *producer = (Producer *) arg;
    int counter[3] = {0, 0, 0};
    for (int i = 0; i < producer->production_number; ++i) {
        //1. read news items from the input file
        NewsItem *item = malloc(sizeof(NewsItem));
        item->producer_id = producer->id;
        item->type = i % 3;
        counter[i % 3]++;
        item->production_number = counter[i % 3];
        //2. enqueue news items into the bounded queue
        bounded_enqueue(producer->bounded_queue, item);
    }
    //Add last item
    NewsItem *lastItem = malloc(sizeof(NewsItem));
    lastItem->producer_id = producer->id;
    lastItem->type = -1;
    lastItem->production_number = 1;
    bounded_enqueue(producer->bounded_queue, lastItem);
    return NULL;
}

void *dispatcherFunc(void *arg) {
    Dispatcher *dispatcher = (Dispatcher *) arg;
    int lastArticle[dispatcher->numProducers];
    for (int i = 0; i < dispatcher->numProducers; ++i) {
        lastArticle[i] = 0;
    }
    int end = 0;
    BoundedQueue **producerQueue = dispatcher->producer_queue;
    UnboundedQueue **coEditorQueue = dispatcher->coEditor_queue;
    while (end < dispatcher->numProducers) {
        //1. dequeue news items from the bounded queue of each producer (round robin)
        for (int i = 0; i < dispatcher->numProducers; ++i) {
            if (lastArticle[i] != -1) {
                BoundedQueue *producerQueueItem = producerQueue[i];
                NewsItem item = bounded_dequeue(producerQueueItem);
                if (item.type == -1) {
                    end++;
                    lastArticle[i] = -1;
                } else {
                    //2. enqueue news items into the unbounded queue
                    UnboundedQueue *dispatcherQueueItem = coEditorQueue[item.type];
                    unbounded_enqueue(dispatcherQueueItem, &item);
                }
            }
        }
    }
    for (int i = 0; i < 3; ++i) {
        UnboundedQueue *dispatcherQueueItem = coEditorQueue[i];
        NewsItem *lastItem = malloc(sizeof(NewsItem));
        lastItem->producer_id = -1;
        lastItem->type = -1;
        lastItem->production_number = 1;
        unbounded_enqueue(dispatcherQueueItem, lastItem);
    }
    return NULL;
}

void *coEditor(void *arg) {
    CoEditor *coEditor = (CoEditor *) arg;
    int end = 0;
    while (end < 1) {
        //1- dequeue from unbounded queue
        NewsItem item = unbounded_dequeue(coEditor->unbounded_queue);
        if (item.type == -1) {
            end++;
        } else {
            //2- enqueue in bounded queue
            bounded_enqueue(coEditor->bounded_queue, &item);
        }
    }
    return NULL;
}

void *screenManager(void *arg) {
    // 1- dequeue from the bounded queue of the coEditor
    BoundedQueue *boundedQueue = (BoundedQueue *) arg;
    int end = 0;
    while (end < 3) {
        NewsItem item = bounded_dequeue(boundedQueue);
        if (item.type == -1) {
            end++;
        } else {
            // 2- print the news item
            switch (item.type) {
                case 0:
                    printf("Producer %i SPORTS %i\n", item.producer_id, item.production_number);
                    break;
                case 1:
                    printf("Producer %i WEATHER %i\n", item.producer_id, item.production_number);
                    break;
                case 2:
                    printf("Producer %i NEWS %i\n", item.producer_id, item.production_number);
                    break;
            }
        }
    }
    return NULL;
}

int main(int argc, char **argv) {
    //check number of arguments
    if (argc != 2) {
        printf("Wrong Number of Arguments\n");
        exit(-1);
    }
    //initialize variables
    ProducerList producerList;
    //size for coEditors bound queue
    int boundq;
    //read the configuration file
    readConfFile(&producerList, argv[1], &boundq);
    //create the bounded queues
    BoundedQueue *bqueueList[producerList.num_of_producers];
    BoundedQueue *coEditerQueue = malloc(sizeof(BoundedQueue));
    if (coEditerQueue == NULL) {
        printf("Error allocating memory for coEditorQueue\n");
        exit(-1);
    }
    createBoundedQueues(&producerList, bqueueList, coEditerQueue, &boundq);
    //create the co editors and their queues
    CoEditorsList coEditorsList = {.num_of_coEditors = 3};
    UnboundedQueue *unboundedQueueList[3];
    createCoEditors(&coEditorsList, unboundedQueueList, coEditerQueue);
    //create the dispatcher
    Dispatcher dispatcher;
    createDispatcher(&dispatcher, &producerList, unboundedQueueList, bqueueList);

    //create the threads id array
    int num_of_threads = producerList.num_of_producers + 5;
    pthread_t *threads = malloc(num_of_threads * sizeof(pthread_t));
    if (threads == NULL) {
        printf("Error allocating memory for threads\n");
        exit(-1);
    }
    //create the producer threads
    for (int i = 0; i < producerList.num_of_producers; ++i) {
        pthread_t producer_thread;
        pthread_create(&producer_thread, NULL, producer, (void *) &producerList.producers[i]);
        threads[i] = producer_thread;
    }
    //create the dispatcher thread
    pthread_t dispatcher_thread;
    pthread_create(&dispatcher_thread, NULL, dispatcherFunc, (void *) &dispatcher);
    threads[producerList.num_of_producers] = dispatcher_thread;

    //create the co editor threads
    for (int i = 0; i < 3; ++i) {
        pthread_t coEditor_thread;
        pthread_create(&coEditor_thread, NULL, coEditor, (void *) &coEditorsList.coEditors[i]);
        threads[i + producerList.num_of_producers + 1] = coEditor_thread;
    }
    //create the screen manager thread
    pthread_t screenManager_thread;
    pthread_create(&screenManager_thread, NULL, screenManager, (void *) coEditerQueue);

    //join all the threads after they are done running
    for (int i = 0; i < num_of_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    free(threads);
    free(producerList.producers);
    free(coEditerQueue);
    for (int i = 0; i < coEditorsList.num_of_coEditors; ++i) {
        free(coEditorsList.coEditors[i].unbounded_queue);
    }
    for (int i = 0; i < producerList.num_of_producers; ++i) {
        free(bqueueList[i]);
    }
    free(coEditorsList.coEditors);
    exit(0);
}
