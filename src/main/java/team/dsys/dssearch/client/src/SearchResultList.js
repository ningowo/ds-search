import React from 'react';

function SearchResulList({ results }) {
    if (results == null) {
        return (
            <div>
                <h3>Waiting...</h3>
            </div>
        );
    }
    const renderedResults = Object.values(results).map(result => {
        return (//jsx; react requires to set key
            <div className="card" style={{width: "30%", marginBottom: "20px"}} key={result.id}>
                <div className="card-body">
                    <h3>{result.data}</h3>
                </div>
            </div>
        );
    }) //give all the elements into an array

    //display renderedPosts
    return (
        <div className="d-flex flex-row flex-wrap justify-content-between">
            {renderedResults}
        </div>
    );

}

export default SearchResulList;