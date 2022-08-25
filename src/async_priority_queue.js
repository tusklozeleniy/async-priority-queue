const defer = require('promise-defer');

const AsyncPriorityQueue = ({
  debug = false,
  maxParallel = 6,
  processingFrequency = 30,
  priorities = ['high', 'mid', 'low'],
  defaultPriority = priorities[Math.floor(priorities.length / 2)],
} = {}) => {
  let q = priorities.reduce((acc, priority) => {
    acc[priority] = [];
    return acc;
  }, {});

  let active = [], interval;
  debug && console.log(`instantiating AsyncPriorityQueue with max parallelism of ${maxParallel} and processingFrequency of ${processingFrequency} ms`);

  const cleanup = (task) => () => {
    debug && console.log('removing resolved task from the active task list');
    active.splice(active.indexOf(task));
  };

  return {
    start() { interval = setInterval(this.processQueue, processingFrequency); },
    stop() { interval && clearInterval(interval); },
    clear(priority) { q[priority] = []; },
    enqueue(task) {
      const { priority = defaultPriority} = task;
      q[priority] && q[priority].unshift(task) || console.error(`Invalid priority: ${priority}. Possible values: '${priorities.join('\', \'')}'`);
    },
    processQueue() {
      debug && console.log('processing task queue');
      debug && console.log('active:', active.length);
      if (active.length <= maxParallel) {
        debug && console.log(priorities.map((p) => `${p}: ${q[p].length}`).join(', '));

        const maxPriorityWithTasks = priorities.find((p) => q[p].length > 0);
        let activeTask = q[maxPriorityWithTasks] && q[maxPriorityWithTasks].pop();
        if (activeTask) {
          debug && console.log('executing new task');
          active.push(activeTask);
          activeTask.execute().then(cleanup(activeTask), cleanup(activeTask));
        }
      }
    }
  };
};

const AsyncTask = (config = {}) => {
  const {priority, callback} = config;
  const deferred = defer();
  return {
    priority,
    promise: deferred.promise,
    execute() {
      return callback().then((data) => deferred.resolve(data), (data) => deferred.reject(data));
    }
  };
};

module.exports = {AsyncPriorityQueue, AsyncTask};