//create app component here and then import it in the index.js file and then render it into some DOM element
import React from 'react';
import SearchCreate from './SearchCreate';
import SearchResultList from "./SearchResultList";

const App = () => {
    return (
        //give class name container to have some constrain on the edge
        <div className="container">
            <h1>Simple Search</h1>
            <SearchCreate />
            <hr />
            <h3>Search Result</h3>
        </div>
    );
};
export default App;
