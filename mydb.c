#include "mydb.h"
#define MAX_KEY_DATA_SIZE 50

int db_close(struct DB *db) {
	return db->close(db);
}

int db_delete(struct DB *db, void *key, size_t key_len) {
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	return db->delete(db, &keyt);
}

int db_select(struct DB *db, void *key, size_t key_len,
	   void **val, size_t *val_len) {
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	struct DBT valt = {0, 0};
	int rc = db->select(db, &keyt, &valt);
	*val = valt.data;
	*val_len = valt.size;
	return rc;
}

int db_insert(struct DB *db, void *key, size_t key_len,
	   void *val, size_t val_len) {
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	struct DBT valt = {
		.data = val,
		.size = val_len
	};
	return db->insert(db, &keyt, &valt);
}

struct DB *dbopen(const char *filename, struct DBC *conf){
	struct DB *db = (struct DB *)malloc(sizeof(struct DB));
	printf("Opening file\n");
	if ((db->fd = open(fielanem, O_RDWR)) == -1){
		printf("Creating a new file\n");
		if ((db->fd = open("db", O_RDWR || O_TRUNC)) == -1)
			printf("%s\n",strerror(errno));
		//write(db.fd, conf->db_size, sizeof(conf->db_size));
		//write(db.fd, conf->page_size, sizeof(conf->page_size));
		//write(db.fd, conf->cache_size, sizeof(conf->cache_size));
		db->DB_prm = *conf;
		if (fallocate(db->fd, 0, 0, conf->db_size, 0) == -1)
			printf("%s\n", strerror(errno));
		db->blocks_num = (conf->db_size - sizeof(conf)) / conf->page_size - 1;
		if (write(db->fd, &db->DB_prm, sizeof(db->DB_prm)) == -1)
			printf("%s\n", strerror(errno));
		db->blocks = (char *)malloc(db->blocks_num);
		for (int i = 0; i < db->blocks_num; i++) db->blocks[i] = 0;
		db->zeroBlockOffs = sizeof(db->DB_prm) + db->blocks_num*sizeof(int);
	}
	else{
		//...
		if (read(db->fd, &db->DB_prm, sizeof(db->DB_prm)) == -1)
			printf("%s\n", strerror(errno));
		db->blocks_num = (db->DB_prm.db_size - sizeof(db->DB_prm)) / conf->page_size;
		db->blocks = (char *)malloc(db->blocks_num);
		if (read(db->fd, &db->blocks, db->blocks_num) == -1) printf("%s\n", strerror(errno));
		db->zeroBlockOffs = sizeof(db->DB_prm) + db->blocks_num;
	}
	return db;
}

int close(struct DB *db){
#if 0
	if ((db->fd = open("db", O_RDWR || O_TRUNC)) == -1)	printf("%s\n", strerror(errno));
#endif
		return 0;

}

int insert_to_block(struct DB *db, int block_num, struct DBT *key, struct DBT *data, int left_block_num, int right_block_num){
	// leaf
	// [key1 || data1 || key2 || data2 || ... || data1_size || key1_size]

	// other nodes
	// [block_num0 || key1 || data1 || block_num1 || key2 || data2 || block_num2 || ... || data1_size || key1_size]

	int plen = is_leaf(db, block_num)*sizeof(int); // =0 in case of leaf, sizeof(int) otherwise

	if (key->size + data->size > MAX_KEY_DATA_SIZE){
		printf("Make the pair key/data shorter to avoid problems\n");
		return -1;
	}

	// Is there space to insert?
	if (db->DB_prm.page_size - db->blocks[block_num] < key->size + data->size + plen){
		printf("Cannot insert: the pair key/data is too long => make MAX_KEY_DATA_SIZE smaller\n");
		return -1;
	}

	// Read the whole block into block_buf
	char *block_buf = (void *)malloc(db->DB_prm.page_size);
	lseek(db->fd, db->zeroBlockOffs + block_num*db->DB_prm.page_size);
	if (read(db->fd, block_buf, db->DB_prm.page_size) == -1) printf("%s\n", strerror(errno));

	// Search where to insert
	size_t dataOffs = plen, sizeOffs = db->DB_prm.page_size - 2*sizeof(size_t);
	size_t curr_key_size, curr_data_size;
	while (1){
		// What's the next key/block sizes?
		curr_data_size = *(size_t *)(block_buf + sizeOffs);
		curr_key_size = *(size_t *)(block_buf + sizeOffs + sizeof(size_t));
		if ( curr_key_size != 0){ 
			// Is current key greater or smaller?
			int min_len = (curr_key_size > key->size) ? key->size : curr_key_size;
			int res = memcmp(block_buf + dataOffs, key->data, min_len);
			if (res < 0 || res == 0 && curr_key_size < key->size){
				dataOffs += curr_key_size + curr_data_size + plen;
				sizeOffs -= 2 * sizeof(size_t);
				continue;
			}
			if (res = 0 && key->size == curr_key_size) printf("This key already exists!\n");
			// The key is greater => let's move it!
			// How many bytes to move?
			size_t moveSize = curr_key_size + curr_data_size + plen;
			size_t sizeOffsTmp = sizeOffs - 2 * sizeof(size_t);
			while (1){
				curr_data_size = *(size_t *)(block_buf + sizeOffs);
				curr_key_size = *(size_t *)(block_buf + sizeOffs + sizeof(size_t));
				if (curr_key_size == 0) break;
				moveSize += curr_key_size + curr_data_size + plen;
				sizeOffsTmp -= 2 * sizeof(size_t);
			}
			// Moving
			memmove(block_buf + dataOffs + key->size + data->size + plen, block_buf + dataOffs, moveSize);
			memmove(block_buf + sizeOffsTmp, block_buf + sizeOffsTmp + 2 * sizeof(size_t), sizeOffs - (sizeOffsTmp + 2 * sizeof(size_t)));
		}
		else{ // no more keys left

		}
		// Inserting key/data
		dataOffs -= plen;
		if (left_block_num != 0){
			memcpy(block_buf + dataOffs, &left_block_num, sizeof(int));
			memcpy(block_buf + dataOffs + sizeof(int) + key->size + data->size, &right_block_num, sizeof(int));
		}
		memcpy(block_buf + dataOffs + plen, key->data, key->size);
		memcpy(block_buf + dataOffs + key->size + plen, data->data, data->size);
		memcpy(block_buf + sizeOffs + 1, &key->size, sizeof(size_t));
		memcpy(block_buf + sizeOffs, &data->size, sizeof(size_t));
		db->blocks[block_num] += key->size + data->size + 2*sizeof(size_t) + plen;
		break;
	}
	lseek(db->fd, db->zeroBlockOffs + block_num*db->DB_prm.page_size);
	if (write(db->fd, block_buf, db->DB_prm.page_size) == -1) printf("%s\n", strerror(errno));
	free(block_buf);
}

int find_subtree(struct DB *db, int block_num, struct DBT *key){

	int plen = sizeof(int);

	// Read the whole block into block_buf
	char *block_buf = (char *)malloc(db->DB_prm.page_size);
	lseek(db->fd, db->zeroBlockOffs + block_num*db->DB_prm.page_size);
	if (read(db->fd, block_buf, db->DB_prm.page_size) == -1) printf("%s\n", strerror(errno));

	// Search where the key is
	size_t dataOffs = plen, sizeOffs = db->DB_prm.page_size - 2 * sizeof(size_t);
	size_t curr_key_size, curr_data_size;
	while (1){
		curr_data_size = *(size_t *)(block_buf + sizeOffs);
		curr_key_size = *(size_t *)(block_buf + sizeOffs + sizeof(size_t));
		if (curr_key_size == 0) break;
		// Is current key greater or smaller?
		int min_len = (curr_key_size > key->size) ? key->size : curr_key_size;
		int res = memcmp(block_buf + dataOffs, key->data, min_len);
		if (res < 0 || res == 0 && curr_key_size < key->size){
			dataOffs += curr_key_size + curr_data_size + plen;
			sizeOffs -= 2 * sizeof(size_t);
			continue;
		}
		if (res = 0 && key->size == curr_key_size){
			printf("This key already exists!\n");
			return -1;
		}
		// The key is greater => We have found the subtree
		break;
	}
	int new_block_num = *(int *)(block_buf + dataOffs - plen);
	free(block_buf);
	return new_block_num;
}

int split_block(struct DB *db, int new_block_num, int block_num){

	// Read the whole blocks into buffers
	char *left_child_buf = (char *)malloc(db->DB_prm.page_size);
	lseek(db->fd, db->zeroBlockOffs + new_block_num*db->DB_prm.page_size);
	if (read(db->fd, left_child_buf, db->DB_prm.page_size) == -1) printf("%s\n", strerror(errno));

	char *right_child_buf = (char *)malloc(db->DB_prm.page_size);

	int plen = is_leaf(db, new_block_num)*sizeof(int); // =0 in case of leaf, sizeof(int) otherwise

	// Search where the middle of the block is
	size_t dataOffs = plen, sizeOffs = db->DB_prm.page_size - 2 * sizeof(size_t);
	size_t curr_key_size, curr_data_size;

	while (dataOffs < db->DB_prm.page_size / 2){
		curr_data_size = *(size_t *)(left_child_buf + sizeOffs);
		curr_key_size = *(size_t *)(left_child_buf + sizeOffs + sizeof(size_t));
		if (curr_key_size == 0){
			printf("MAX_KEY_DATA_SIZE is too big!\n");
			return -1;
		}
		dataOffs += curr_key_size + curr_data_size + plen;
		sizeOffs -= 2 * sizeof(size_t);
	}

	struct DBT key, data;

	key.size = curr_key_size;
	key.data = (void *)malloc(curr_key_size);
	memcpy(key.data, left_child_buf + dataOffs, curr_key_size);

	data.size = curr_data_size;
	data.data = (void *)malloc(curr_data_size);
	memcpy(data.data, left_child_buf + dataOffs + curr_key_size, curr_data_size);

	// The middle is found. Let's split
	size_t dataMoveSize = curr_key_size + curr_data_size + plen;
	long sizeOffsTmp = sizeOffs - 2 * sizeof(size_t);
	while (1){
		curr_data_size = *(size_t *)(left_child_buf + sizeOffs);
		curr_key_size = *(size_t *)(left_child_buf + sizeOffs + sizeof(size_t));
		if (curr_key_size == 0) break;
		dataMoveSize += left_child_buf[sizeOffsTmp] + left_child_buf[sizeOffsTmp + 1] + plen;
		sizeOffsTmp -= 2 * sizeof(size_t);
	}
	// Moving to the right block
	size_t sizeMoveSize = sizeOffs - (sizeOffsTmp + 2 * sizeof(size_t));
	memcpy(right_child_buf, left_child_buf + dataOffs + key.size + data.size, dataMoveSize);
	memcpy(right_child_buf + db->DB_prm.page_size - sizeMoveSize, left_child_buf + sizeOffsTmp + 2 * sizeof(size_t), sizeMoveSize);

	// Erasing the right part of the left block
	int max_len = (dataMoveSize < sizeMoveSize) ? sizeMoveSize : dataMoveSize;
	void *zeros = calloc(max_len, sizeof(char));
	memcpy(left_child_buf + dataOffs + key.size + data.size, zeros, dataMoveSize);
	memcpy(left_child_buf + sizeOffsTmp + 2 * sizeof(size_t), zeros, sizeMoveSize);

	lseek(db->fd, db->zeroBlockOffs + new_block_num*db->DB_prm.page_size);
	if (write(db->fd, left_child_buf, db->DB_prm.page_size) == -1) printf("%s\n", strerror(errno));
	// Searching for a free block
	for (int i = 0; i < db->blocks_num; i++){
		if (db->blocks[i] == 0){
			lseek(db->fd, db->zeroBlockOffs + i*db->DB_prm.page_size);
			if (write(db->fd, right_child_buf, db->DB_prm.page_size) == -1) printf("%s\n", strerror(errno));

			if (insert_to_block(db, block_num, &key, &data, new_block_num, i) == -1) return -1;

			if (is_leaf(db, new_block_num)){
				db->blocks[i] = sizeMoveSize + dataMoveSize;
				db->blocks[new_block_num] -= sizeMoveSize + dataMoveSize;
			}
			else{
				db->blocks[i] = -sizeMoveSize - dataMoveSize;
				db->blocks[new_block_num] += sizeMoveSize + dataMoveSize;
			}
			free(left_child_buf);
			free(right_child_buf);
			return 0;
		}
	}
	printf("No free blocks left\n");
	return -1;
}

int is_full(struct DB *db, int block_num){
	return (db->DB_prm.page_size - abs(db->blocks[block_num]) < MAX_KEY_DATA_SIZE) ? 1 : 0;
}

int is_leaf(struct DB *db, int block_num){
	return (db->blocks[block_num] > 0) ? 1 : 0;
}

int insert_to_subtree(struct DB *db, int block_num, struct DBT *key, struct DBT *data){
	if (is_leaf(db, block_num)){
		return insert_to_block(db, block_num, key, data, 0, 0);
	}
	else{
		int new_block_num = find_subtree(db, block_num, key);
		if (insert_to_subtree(db, new_block_num, key, data) == -1) return -1;
		if (is_full(db, new_block_num)){
			return split_block(db, new_block_num, block_num);
		}
	}
}

int insert(struct DB *db, struct DBT *key, struct DBT *data){
	if (insert_to_subtree(db, 0, key, data) == -1) return -1;
	if (is_full(db, 0)){
		return split_block(db, 0, 0);
	}
	return 0;
}


int find_element(struct DB *db, int block_num, struct DBT *key, struct DBT *data){

	int plen = is_leaf(db, block_num)*sizeof(int);

	// Read the whole block into block_buf
	char *block_buf = (char *)malloc(db->DB_prm.page_size);
	lseek(db->fd, db->zeroBlockOffs + block_num*db->DB_prm.page_size);
	if (read(db->fd, block_buf, db->DB_prm.page_size) == -1) printf("%s\n", strerror(errno));

	// Search where the key is
	size_t dataOffs = plen, sizeOffs = db->DB_prm.page_size - 2 * sizeof(size_t);
	size_t curr_key_size, curr_data_size;
	while (1){
		curr_data_size = *(size_t *)(block_buf + sizeOffs);
		curr_key_size = *(size_t *)(block_buf + sizeOffs + sizeof(size_t));
		if (curr_key_size == 0) break;
		// Is current key greater or smaller?
		int min_len = (curr_key_size > key->size) ? key->size : curr_key_size;
		int res = memcmp(block_buf + dataOffs, key->data, min_len);
		if (res < 0 || res == 0 && curr_key_size < key->size){
			dataOffs += curr_key_size + curr_data_size + plen;
			sizeOffs -= 2 * sizeof(size_t);
			continue;
		}
		if (res = 0 && key->size == curr_key_size){
			data->size = curr_data_size;
			data->data = malloc(curr_data_size);
			memcpy(data->data, block_buf + dataOffs + curr_key_size, curr_data_size);
			return 0;
		}
		// The key is greater => We have found the subtree
		break;
	}
	if (!is_leaf(db,block_num)){
		int new_block_num = *(int *)(block_buf + dataOffs - plen);
		free(block_buf);
		find_element(db, new_block_num, key, data);
	}
	else{
		free(block_buf);
		printf("No such key in the database\n");
		return -1;
	}
}

int select(struct DB *db, struct DBT *key, struct DBT *data){
	return find_element(db, 0, key, data);
}

/*
int insert_to_block(struct DB *db, int block_num, struct DBT *key, struct DBT *data){
	// [key1 || data1 || block_num1 || key2 || data2 || block_num2 || ... || data1_size || key1_size]
	int size_buf[2];
	char data_buf[100];
	int k = 1;
	long offset = 0;
	// Is there space to insert?
	if (db->DB_prm.page_size - db->blocks[block_num] < key->size + data->size){
		printf("The pair key/data is too long!\n");
		return -1;
	}
	while(1){
		// What's the next key/block sizes?
		lseek(db->fd, db->zeroBlockOffs + (block_num + 1)*db->DB_prm.page_size -2*k*sizeof(size_t), 0);
		if (read(db->fd, size_buf, 2 * sizeof(size_t)) == -1) printf("%s\n", strerror(errno));
		k++;
		if (size_buf[1] == 0){ // no more keys left
			lseek(db->fd, db->zeroBlockOffs + (block_num + 1)*db->DB_prm.page_size - 2 * k*sizeof(size_t), 0);
			if (write(db->fd, data->size, sizeof(size_t)) == -1) printf("%s\n", strerror(errno));
			if (write(db->fd, key->size, sizeof(size_t)) == -1) printf("%s\n", strerror(errno));
			lseek(db->fd, db->zeroBlockOffs + block_num*db->DB_prm.page_size + offset, 0);
			if (write(db->fd, key->data, key->size) == -1) printf("%s\n", strerror(errno));
			if (write(db->fd, data->data, data->size) == -1) printf("%s\n", strerror(errno));
			db->blocks[block_num] += key->size + data->size;
			return 0;
		}
		offset += size_buf[0] + size_buf[1];
		// What's the current key?
		lseek(db->fd, db->zeroBlockOffs + block_num*db->DB_prm.page_size + offset, 0);
		if (read(db->fd, data_buf, size_buf[1]) == -1) printf("%s\n", strerror(errno));
		// Is it greater or smaller?
		int min_len = (size_buf[1] > key->size) ? key->size : size_buf[1];
		int res;
		if ((res = memcmp(data_buf, key->data, min_len)) < 0) continue;
		if (res = 0 && key->size == size_buf[1]) printf("This key already exists!\n");
		// The key is greater => let's move it!
		// How many bytes to move?
		int moveSize = size_buf[0] + size_buf[1];
		do{
			lseek(db->fd, db->zeroBlockOffs + (block_num + 1)*db->DB_prm.page_size - 2 * k*sizeof(size_t), 0);
			if (read(db->fd, size_buf, 2 * sizeof(size_t)) == -1) printf("%s\n", strerror(errno));
			moveSize += size_buf[0] + size_buf[1];
			k++;
		} while (size_buf[1] != 0);
		// Moving

		if (write(db->fd, data->size, sizeof(size_t)) == -1) printf("%s\n", strerror(errno));
		if (write(db->fd, key->size, sizeof(size_t)) == -1) printf("%s\n", strerror(errno));
		lseek(db->fd, db->zeroBlockOffs + block_num*db->DB_prm.page_size + offset, 0);
		if (read(db->fd, data_buf, moveSize) == -1) printf("%s\n", strerror(errno));

		lseek(db->fd, db->zeroBlockOffs + block_num*db->DB_prm.page_size + offset, 0);

		if (size_buf[1] == 0){	// key size == 0
			lseek(db->fd, db->zeroBlockOffs + (block_num + 1)*db->DB_prm.page_size - 2 * k*sizeof(size_t), 0);
			if (write(db->fd, data->size, sizeof(size_t)) == -1) printf("%s\n", strerror(errno));
			if (write(db->fd, key->size, sizeof(size_t)) == -1) printf("%s\n", strerror(errno));
			lseek(db->fd, db->zeroBlockOffs + block_num*db->DB_prm.page_size + offset, 0);
			if (write(db->fd, key->data, key->size) == -1) printf("%s\n", strerror(errno));
			if (write(db->fd, data->data, data->size) == -1) printf("%s\n", strerror(errno));
		}
	}
}
*/
