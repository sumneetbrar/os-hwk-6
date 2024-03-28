#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "ts_hashmap.h"

/**
 * Creates a new thread-safe hashmap.
 *
 * @param capacity initial capacity of the hashmap.
 * @return a pointer to a new thread-safe hashmap.
 */
ts_hashmap_t *initmap(int capacity) {
  ts_hashmap_t *map = (ts_hashmap_t *)malloc(sizeof(ts_hashmap_t));
  if (!map) return NULL; // if allocation failed

  // initalize hashmap
  map->capacity = capacity;
  map->size = 0;
  map->numOps = 0;

  map->table = (ts_entry_t **)malloc(sizeof(ts_entry_t *) * capacity);
  if (!map->table) {
    free(map);
    return NULL;
  }

  // initialize table pointers to NULL
  for (int i = 0; i < capacity; i++) {
    map->table[i] = NULL;
  }

  // initialize the lock
  if (pthread_mutex_init(&map->lock, NULL) != 0) {
    free(map->table);
    free(map);
    return NULL;
  }

  return map;
}

/**
 * Obtains the value associated with the given key.
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int get(ts_hashmap_t *map, int key) {
  unsigned int index = (unsigned int)key % map->capacity; // calculate index
  pthread_mutex_lock(&map->lock);
  ts_entry_t *entry = map->table[index];
  while (entry != NULL) {
    if (entry->key == key) {
      int value = entry->value; 
      map->numOps++; 
      pthread_mutex_unlock(&map->lock); // unlock before returning
      return value; // return the found value
    }
    entry = entry->next; // move to next
  }
  map->numOps++; // increment ops

  pthread_mutex_unlock(&map->lock); // unlock
  return INT_MAX;
}

/**
 * Associates a value associated with a given key.
 * @param map a pointer to the map
 * @param key a key
 * @param value a value
 * @return old associated value, or INT_MAX if the key was new
 */
int put(ts_hashmap_t *map, int key, int value) {
  int oldValue = INT_MAX;
  unsigned int index = (unsigned int)key % map->capacity; // calculate index

  pthread_mutex_lock(&map->lock); // lock on

  ts_entry_t **ptr = &(map->table[index]);
  while (*ptr != NULL) {
    if ((*ptr)->key == key) {
      oldValue = (*ptr)->value;
      (*ptr)->value = value;
      map->numOps++;
      pthread_mutex_unlock(&map->lock); // unlock before returning
      return oldValue;
    }
    ptr = &((*ptr)->next); // move on
  }

  // If key doesn't exist, create a new entry and insert it
  ts_entry_t *newEntry = (ts_entry_t *)malloc(sizeof(ts_entry_t));
  if (newEntry) {
    newEntry->key = key;
    newEntry->value = value;
    newEntry->next = map->table[index]; // insert at the head
    map->table[index] = newEntry;
    map->size++; 
  }
  map->numOps++;

  pthread_mutex_unlock(&map->lock); // unlock before returning

  return oldValue;
}

/**
 * Removes an entry in the map
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int del(ts_hashmap_t *map, int key) {
  unsigned int index = (unsigned int)key % map->capacity; // calculate index
  int value = INT_MAX;

  pthread_mutex_lock(&map->lock); // lock on

  ts_entry_t **ptr = &(map->table[index]);
  while (*ptr != NULL) {
    // If the key is found
    if ((*ptr)->key == key) {
      ts_entry_t *temp = *ptr; 
      value = temp->value; 

      *ptr = temp->next;

      free(temp);    
      map->size--;   
      map->numOps++;

      pthread_mutex_unlock(&map->lock); // lock off
      return value;                
    }
    ptr = &((*ptr)->next); // move to next
  }
  map->numOps++;

  pthread_mutex_unlock(&map->lock); // lock off
  return value;                     // return int max if the key is not found
}

/**
 * Prints the contents of the map (given)
 */
void printmap(ts_hashmap_t *map) {
  for (int i = 0; i < map->capacity; i++)
  {
    printf("[%d] -> ", i);
    ts_entry_t *entry = map->table[i];
    while (entry != NULL)
    {
      printf("(%d,%d)", entry->key, entry->value);
      if (entry->next != NULL)
        printf(" -> ");
      entry = entry->next;
    }
    printf("\n");
  }
}

/**
 * Free up the space allocated for hashmap
 * @param map a pointer to the map
 */
void freeMap(ts_hashmap_t *map) {
  if (map == NULL) return;
  pthread_mutex_lock(&map->lock);

  // iterate through each bucket
  for (int i = 0; i < map->capacity; i++) {
    ts_entry_t *entry = map->table[i];
    while (entry != NULL) {
      ts_entry_t *next = entry->next;
      free(entry);  // Free the current entry
      entry = next; // Move to the next entry
    }
  }
  free(map->table);
  pthread_mutex_unlock(&map->lock); // unlock
  pthread_mutex_destroy(&map->lock); // and destroy
  free(map);
}