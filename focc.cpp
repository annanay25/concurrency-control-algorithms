#include <stdio.h>
#include <iostream>
#include <fstream>
#include <string>
#include <stdlib.h>
#include <pthread.h>
#include <shared_mutex>
#include <vector>
#include <unistd.h>
#include <algorithm>

using namespace std;

shared_mutex cs; 

/* rw - read / write
 * var - variable name
 * tin - transaction number.
 * ex - executed?
 * */
struct op_info{
  char rw;
  char var;
  int tin;
  bool ex;
};

vector<op_info> op_list;

void mark_op_complete(int pos)
{
  // cout << "Hello from mark_op_complete() " << endl;

  while(!cs.try_lock()){
    /* Spin. */
  }

  /* Mark ex as true in the given pos.
   * */ 
  op_list[pos].ex = true;

  cout << op_list[pos].rw << op_list[pos].tin << op_list[pos].var << endl;
  sleep(2);

  cs.unlock();
  return;
}

/* One write, no reads. */
void write_fun(int tid, vector<int> read_set)
{
  while(!cs.try_lock()){
    /* Spin */
  }

  // cout << "I'm in write_fun()" << endl;
  /* Do write.
   *
   * Phase 1 : Validate.
   * Among the transactions that are not complete 
   *    scan op_list for writes, if there's even one != 1 then its not complete.
   * If RS(tj) ∩ WS(ti) = ∅
   *    Then goto phase 2, otherwise abort.
   *
   * Phase 2 : Write and commit.
   * */

  vector<int> incomplete_txn;
  for(vector<op_info>::size_type i = 0; i != op_list.size(); i++) {
    if(op_list[i].tin != tid && op_list[i].rw == 'w' && !op_list[i].ex ){
      int flag = 0;
      for(vector<int>::size_type j = 0; j != incomplete_txn.size(); j++) {
        if(incomplete_txn[j] == op_list[i].tin){
          flag = 1;
        }
      }
      if(flag == 0)
        incomplete_txn.push_back(op_list[i].tin);
    }
  }

  for(auto const& txn: incomplete_txn) {
    /* Get the write set of this transaction. */
    vector<int> write_set; 
    for(vector<op_info>::size_type i = 0; i != op_list.size(); i++) {
      /* Push back positions of the write set of this transaction. */
      if(op_list[i].tin == txn && op_list[i].rw == 'w' && !op_list[i].ex ){
        write_set.push_back(i);
      }
    }

    /* If read set of our txn is in conflict with write set of this transaction
     * Abort. */
    for(auto const& read_op: read_set){
      int pos = read_op;
      char myChar = op_list[pos].var;
      auto it = find_if(
          write_set.begin(), write_set.end(), [&myChar](const int& obj) {return op_list[obj].var == myChar;});

      if(it != write_set.end()){
        /* Abort this txn. */
        cout << "[Abort] " << tid << endl;
        cs.unlock();
        return;
      }
    }

  }

  /* If not, then execute the write operations of our tid. */
  for(vector<op_info>::size_type i = 0; i != op_list.size(); i++) {
    if(op_list[i].tin == tid && op_list[i].rw == 'w' && !op_list[i].ex ){
      /* Mark ex as true in the given pos.
       * */ 
      op_list[i].ex = true;

      cout << op_list[i].rw << op_list[i].tin << op_list[i].var << endl;
      sleep(2);
    }
  }

  cout << "[Commit] " << tid << endl;
  /* Finally, unlock. */
  cs.unlock();
}

/* Multiple reads, no write */
vector< int > get_read_set(int tid){

  // shared_lock<shared_mutex> lock(cs);
  while(!cs.try_lock_shared()){
    /* Spin */
  }

  /* Do read. 
   *
   * Read through the array for reads of this tid. 
   * Reply with array of read set. 
   * */ 

  vector< int > ret;
  for(vector<op_info>::size_type i = 0; i != op_list.size(); i++) {

    if(op_list[i].tin == tid && op_list[i].rw == 'r'){
      ret.push_back(i);
    }

  }

  // shared_lock<shared_mutex> unlock(cs);
  cs.unlock_shared();
  return ret;
}

bool validate(int tid, vector<int> write_set){

  while(!cs.try_lock_shared()){
    /* Spin */
  }

  /* Do read. 
   *
   * Among the transactions that are still reading 
   *    scan op_list for writes, if there's even one != 1 then it's reading.
   * If WS(tj) ∩ RS(ti) = ∅
   *    Then goto next phase, otherwise abort.
   * */ 

  vector<int> incomplete_txn;
  for(vector<op_info>::size_type i = 0; i != op_list.size(); i++) {
    if(op_list[i].tin != tid && op_list[i].rw == 'w' && !op_list[i].ex ){
      int flag = 0;
      for(vector<int>::size_type j = 0; j != incomplete_txn.size(); j++) {
        if(incomplete_txn[j] == op_list[i].tin){
          flag = 1;
        }
      }
      if(flag == 0)
        incomplete_txn.push_back(op_list[i].tin);
    }
  }


  for(auto const& txn: incomplete_txn) {

    /* Get the read set of this transaction. */
    vector<int> read_set; 
    for(vector<op_info>::size_type i = 0; i != op_list.size(); i++) {
      /* Push back positions of the read set of this transaction. */
      if(op_list[i].tin == txn && op_list[i].rw == 'r' ){
        read_set.push_back(i);
      }
    }

    /* If write set of our txn is in conflict with read_set of this transaction
     * Abort. */
    for(auto const& write_op: write_set){
      int pos = write_op;
      char myChar = op_list[pos].var;
      auto it = find_if(
          read_set.begin(), read_set.end(), [&myChar](const int& obj) {return op_list[obj].var == myChar;});

      if(it != read_set.end()){
        /* Abort our txn. */
        cout << "[Abort, focc] " << tid << endl;
        cs.unlock_shared();
        return true;
      }
    }

  }

  cs.unlock_shared();
  return false;
}

vector<int> get_write_set(int tid){
  while(!cs.try_lock_shared()){
    /* Spin */
  }

  /* Get the write set of our transaction. */
  vector<int> write_set; 
  for(vector<op_info>::size_type i = 0; i != op_list.size(); i++) {
    /* Push back positions of the write set of our transaction. */
    if(op_list[i].tin == tid && op_list[i].rw == 'w' ){
      write_set.push_back(i);
    }
  }

  cs.unlock_shared();
  return write_set;
}

struct argsForCStest {
  int tid;
  int thread_num;
};

void *CSTest(void *argStruct){
  /* Each thread will first scan the array for its operations.
   * Create a local index of the reads and then write in their [2] slots 
   * during the write phase. */

  struct argsForCStest args  =  *(argsForCStest *) argStruct;

  vector< int > read_set = get_read_set(args.tid + 1);
  vector< int > write_set = get_write_set(args.tid + 1);

  // cout << "Tid " << args.tid << " has " << read_set.size() << " read ops" << endl;

  /* Fine-grained read.
   * Scan read_set, mark all read ops as complete. */
  bool abrt = false;
  for(vector<int>::size_type i = 0; i != read_set.size() ; i++) {
    mark_op_complete(read_set[i]);
    abrt = validate(args.tid+1, write_set);
    if(abrt)
      break;
  }

  /* Coarse-grained lock now.
   * Begin val-write phase.*/
  if(!abrt)
    write_fun(args.tid + 1, read_set);
}

int main(){
  ifstream myfile;
  myfile.open ("sample-schedule.txt");

  string single_op;
  vector<string> inp;

  int max=-1;
  cout << "Input: ";
  while(myfile >> single_op && !myfile.eof()){
    cout << single_op << " ";
    if(single_op.length() == 3){
      if(max < single_op.c_str()[1] - '0')
        max = single_op.c_str()[1] - '0';
      inp.push_back(single_op);
    }
  }
  cout << endl;

  /* Fill op_list.*/
  for(vector<string>::iterator it = inp.begin(); it != inp.end(); ++it) {
    struct op_info obj;
    string temp = *it;
    obj.rw = temp[0];
    obj.tin = temp[1] - '0';
    obj.var = temp[2];
    op_list.push_back(obj);
  }

  /* Thread info. */
  pthread_t threads[max];
  struct argsForCStest threadArgs[max];

  /* Spawn threads. */
  for(int i=0; i<max; i++){
  	threadArgs[i].tid = i;
  	threadArgs[i].thread_num = max-1;
  	pthread_create(&threads[i],NULL,&CSTest,&threadArgs[i]);
  }

  /* Wait for the threads to finish.  */
  for(int i = 0; i < max; i++) {
    pthread_join(threads[i], NULL);
  }

  cout << "Done!" << endl;
  return 0;
}
