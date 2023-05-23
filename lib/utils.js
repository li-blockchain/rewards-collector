// Function which takes a parameter an array of variable size and returns the array split by 100 elements.
function chunkArray(array, size) {
    const chunked_arr = [];
    let index = 0;
    while (index < array.length) {
        chunked_arr.push(array.slice(index, size + index));
        index += size;
    }
    return chunked_arr;
}


module.exports = {
    chunkArray
}