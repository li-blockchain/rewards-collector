// Function which takes a parameter an array of variable size and returns the array split by 100 elements.
function chunkArray(array, size) {
    const chunked_arr = [];
    let index = 0;
    while (index < array.length) {
        chunked_arr.push(array.slice(index, size + index));
        index += size;
    }

    // Build an array of CSV strings for each chunk.
    const validatorChunks = chunked_arr.map((chunk) => {
        let validatorList = '';
        // For each validators get the withdrawals
        for (let i = 0; i < chunk.length; i++) {
            // Build a comma seperated list of validators.
            validatorList += chunk[i].index + ',';
        }

        // Remove the last comma
        validatorList = validatorList.slice(0, -1);
        return validatorList;
    });

    return validatorChunks;
}


function convertNestedArrays(obj) {
    const result = {};
  
    for (const key in obj) {
      if (Array.isArray(obj[key])) {
        result[key] = obj[key].map((arr) => ({ values: arr }));
      } else if (typeof obj[key] === 'object') {
        result[key] = convertNestedArrays(obj[key]);
      } else {
        result[key] = obj[key];
      }
    }
  
    return result;
  }
  

module.exports = {
    chunkArray,
    convertNestedArrays
}