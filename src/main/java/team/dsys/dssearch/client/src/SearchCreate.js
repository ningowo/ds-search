import React, {useState} from 'react';
import axios from 'axios';
import searchResultList from "./SearchResultList";

//create export functional component
const SearchCreate = () => {
    const [title, setTitle] = useState('');
    const [results, setResults] = useState('');

    const onSubmit = async (event) => {
        //prevent default submit
        event.preventDefault();

        const res = await axios.get("http://localhost:8081/s/search?query=apple&size=5");
        setResults(res.data)

        //ux
        //after submit, empty out input
        setTitle("");
    }

    return (
        <div>
            <form onSubmit={onSubmit}>
                <div className="form-group">
                    <label></label>
                    <input
                    value={title}
                    placeholder={"Type here..."}
                    onChange={e => setTitle(e.target.value)}
                    className="form-control"
                    />
                    <button className="btn btn-primary">search</button>
                </div>
            </form>
            {searchResultList(results)}
        </div>
    );
};

export default SearchCreate;