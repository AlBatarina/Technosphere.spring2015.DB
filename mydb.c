#include "mydb.h"


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

struct DB *dbopen(char *file, struct DBC *conf){
	struct DB *Database = (struct DB *)malloc(sizeof(struct DB));
	printf("Opening file\n");
	if ((Database->fd = open("Database", O_RDWR)) == -1){
		printf("Creating new file\n");
		if ((Database->fd = open("Database", O_RDWR || O_TRUNC)) == -1)	printf("%s\n",strerror(errno));
		//write(Database.fd, conf->db_size, sizeof(conf->db_size));
		//write(Database.fd, conf->page_size, sizeof(conf->page_size));
		//write(Database.fd, conf->cache_size, sizeof(conf->cache_size));
		Database->DB_prm = *conf;
		if (fallocate(Database->fd, 0, 0, conf->db_size, 0) == -1) printf("%s\n", strerror(errno));
		Database->blocks_num = (conf->db_size - sizeof(conf)) / conf->page_size - 1;
		if (write(Database->fd, &Database->DB_prm, sizeof(Database->DB_prm)) == -1) printf("%s\n", strerror(errno));
		Database->blocks = (char *)malloc(Database->blocks_num);
		for (int i = 0; i < Database->blocks_num; i++) Database->blocks[i] = 0;
<<<<<<< HEAD
		Database->zeroBlockOffs = sizeof(Database->DB_prm) + Database->blocks_num*sizeof(int);
=======
		Database->zeroBlockOffs = sizeof(Database->DB_prm) + Database->blocks_num;
>>>>>>> 710d1cec363fbfa2ec144ee11bd9828b7d753c1a
	}
	else{
		//...
		if (read(Database->fd, &Database->DB_prm, sizeof(Database->DB_prm)) == -1) printf("%s\n", strerror(errno));
		Database->blocks_num = (Database->DB_prm.db_size - sizeof(Database->DB_prm)) / conf->page_size;
		Database->blocks = (char *)malloc(Database->blocks_num);
		if (read(Database->fd, &Database->blocks, Database->blocks_num) == -1) printf("%s\n", strerror(errno));
		Database->zeroBlockOffs = sizeof(Database->DB_prm) + Database->blocks_num;
	}
	return Database;
}

<<<<<<< HEAD
int insert_to_block(struct DB *db, int block_num, struct DBT *key, struct DBT *data, int left_block_num, int right_block_num){
	// leaf
	// [key1 || data1 || key2 || data2 || ... || data1_size || key1_size]

	// other nodes
	// [block_num0 || key1 || data1 || block_num1 || key2 || data2 || block_num2 || ... || data1_size || key1_size]

	int plen = is_leaf(db, block_num)*sizeof(int); // =0 in case of leaf, sizeof(int) otherwise

	// Is there space to insert?
	if (db->DB_prm.page_size - db->blocks[block_num] < key->size + data->size + plen){
=======
int insert_to_leaf(struct DB *db, int block_num, struct DBT *key, struct DBT *data){
	// [key1 || data1 || block_num1 || key2 || data2 || block_num2 || ... || data1_size || key1_size]

	// Is there space to insert?
	if (db->DB_prm.page_size - db->blocks[block_num] < key->size + data->size){
>>>>>>> 710d1cec363fbfa2ec144ee11bd9828b7d753c1a
		printf("The pair key/data is too long!\n");
		return -1;
	}

<<<<<<< HEAD
	// Read the whole block into block_buf
=======
>>>>>>> 710d1cec363fbfa2ec144ee11bd9828b7d753c1a
	char *block_buf = (char *)malloc(db->DB_prm.page_size);
	lseek(db->fd, db->zeroBlockOffs + block_num*db->DB_prm.page_size);
	if (read(db->fd, block_buf, db->DB_prm.page_size) == -1) printf("%s\n", strerror(errno));

<<<<<<< HEAD
	// Search where to insert
	long dataOffs = plen, sizeOffs = db->DB_prm.page_size - 2*sizeof(size_t);
=======
	long dataOffs = 1, sizeOffs = db->DB_prm.page_size - 2*sizeof(size_t);
>>>>>>> 710d1cec363fbfa2ec144ee11bd9828b7d753c1a
	while (1){
		// What's the next key/block sizes?
		if (block_buf[sizeOffs + 1] != 0){ 
			// Is current key greater or smaller?
			int min_len = (block_buf[sizeOffs + 1] > key->size) ? key->size : block_buf[sizeOffs + 1];
			int res;
<<<<<<< HEAD
			if ((res = memcmp(block_buf + dataOffs, key->data, min_len)) < 0){
				dataOffs += block_buf[sizeOffs] + block_buf[sizeOffs + 1] + plen;
=======
			if ((res = memcmp(block_buf + dataOffs + 1, key->data, min_len)) < 0){
				dataOffs += block_buf[sizeOffs] + block_buf[sizeOffs + 1];
>>>>>>> 710d1cec363fbfa2ec144ee11bd9828b7d753c1a
				sizeOffs -= 2 * sizeof(size_t);
				continue;
			}
			if (res = 0 && key->size == block_buf[sizeOffs + 1]) printf("This key already exists!\n");
			// The key is greater => let's move it!
			// How many bytes to move?
<<<<<<< HEAD
			int moveSize = block_buf[sizeOffs] + block_buf[sizeOffs + 1] + plen;
			long sizeOffsTmp = sizeOffs - 2 * sizeof(size_t);
			while (block_buf[sizeOffsTmp + 1] != 0){
				moveSize += block_buf[sizeOffsTmp] + block_buf[sizeOffsTmp + 1] + plen;
				sizeOffsTmp -= 2 * sizeof(size_t);
			}
			// Moving
			memmove(block_buf + dataOffs + key->size + data->size + plen, block_buf + dataOffs, moveSize);
=======
			dataOffs += block_buf[sizeOffs] + block_buf[sizeOffs + 1];
			int moveSize = block_buf[sizeOffs] + block_buf[sizeOffs + 1];
			dataOffs += block_buf[sizeOffs] + block_buf[sizeOffs + 1];
			long sizeOffsTmp = sizeOffs - 2 * sizeof(size_t);
			while (block_buf[sizeOffsTmp + 1] != 0){
				moveSize += block_buf[sizeOffsTmp] + block_buf[sizeOffsTmp + 1];
				sizeOffsTmp -= 2 * sizeof(size_t);
			}
			// Moving
			memmove(block_buf + dataOffs + key->size + data->size, block_buf + dataOffs, moveSize);
>>>>>>> 710d1cec363fbfa2ec144ee11bd9828b7d753c1a
			memmove(block_buf + sizeOffsTmp, block_buf + sizeOffsTmp + 2 * sizeof(size_t), sizeOffs - (sizeOffsTmp + 2 * sizeof(size_t)));
		}
		else{ // no more keys left

		}
		// Inserting key/data
<<<<<<< HEAD
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
=======
		memcpy(block_buf[dataOffs], key->data, key->size);
		memcpy(block_buf[dataOffs + key->size], data->data, data->size);
		memcpy(block_buf[sizeOffs + 1], key->size, sizeof(size_t));
		memcpy(block_buf[sizeOffs], data->size, sizeof(size_t));
		db->blocks[block_num] += key->size + data->size + 2*sizeof(size_t);
>>>>>>> 710d1cec363fbfa2ec144ee11bd9828b7d753c1a
		break;
	}
	lseek(db->fd, db->zeroBlockOffs + block_num*db->DB_prm.page_size);
	if (write(db->fd, block_buf, db->DB_prm.page_size) == -1) printf("%s\n", strerror(errno));
}
<<<<<<< HEAD

int find_subtree(struct DB *db, int block_num, struct DBT *key){

	int plen = sizeof(int);

	// Read the whole block into block_buf
	char *block_buf = (char *)malloc(db->DB_prm.page_size);
	lseek(db->fd, db->zeroBlockOffs + block_num*db->DB_prm.page_size);
	if (read(db->fd, block_buf, db->DB_prm.page_size) == -1) printf("%s\n", strerror(errno));

	// Search where the key is
	long dataOffs = plen, sizeOffs = db->DB_prm.page_size - 2 * sizeof(size_t);
	while (block_buf[sizeOffs + 1] != 0){
		// Is current key greater or smaller?
		int min_len = (block_buf[sizeOffs + 1] > key->size) ? key->size : block_buf[sizeOffs + 1];
		int res;
		if ((res = memcmp(block_buf + dataOffs, key->data, min_len)) < 0){
			dataOffs += block_buf[sizeOffs] + block_buf[sizeOffs + 1] + plen;
			sizeOffs -= 2 * sizeof(size_t);
			continue;
		}
		if (res = 0 && key->size == block_buf[sizeOffs + 1]) printf("This key already exists!\n");
		// The key is greater => We have found the subtree
		break;
		
	}
		return *(int *)(block_buf + dataOffs - plen);
}

int split_block(struct DB *db, int new_block_num, int block_num){

	// Read the whole blocks into buffers
	char *parent_buf = (char *)malloc(db->DB_prm.page_size);
	lseek(db->fd, db->zeroBlockOffs + block_num*db->DB_prm.page_size);
	if (read(db->fd, parent_buf, db->DB_prm.page_size) == -1) printf("%s\n", strerror(errno));

	char *child_buf = (char *)malloc(db->DB_prm.page_size);
	lseek(db->fd, db->zeroBlockOffs + block_num*db->DB_prm.page_size);
	if (read(db->fd, child_buf, db->DB_prm.page_size) == -1) printf("%s\n", strerror(errno));





	return -1;
}

int is_full(struct DB *db, int block_num){
	return (db->DB_prm.page_size - abs(db->blocks[block_num]) < 50) ? 1 : 0;
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

=======
>>>>>>> 710d1cec363fbfa2ec144ee11bd9828b7d753c1a
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
<<<<<<< HEAD
*/
=======
*/

int split_block(struct DB *db, int new_block_num, int block_num){
	return -1;
}

int is_full(struct DB *db, int block_num){
	return (db->DB_prm.page_size - abs(db->blocks[block_num]) < 50) ? 1 : 0;
}

int is_leaf(struct DB *db, int block_num){
	return (db->blocks[block_num] > 0) ? 1 : 0;
}

int insert_to_subtree(struct DB *db, int block_num, struct DBT *key, struct DBT *data){
	if (is_leaf(db, block_num)){
		return insert_to_leaf(db, block_num, key, data);
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
>>>>>>> 710d1cec363fbfa2ec144ee11bd9828b7d753c1a
