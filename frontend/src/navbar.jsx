import Container from 'react-bootstrap/Container';
import Nav from 'react-bootstrap/Nav';
import Navbar from 'react-bootstrap/Navbar';
import {Link, Route, Routes} from 'react-router-dom';

import VisualSearch from './visualSearch';
import BM25 from './bm25';

function Navigation() {
    return (
        <div>
            <Navbar bg="dark" data-bs-theme="dark">
                <Container>
                <Navbar.Brand as={Link} to="/">Video Retrieval</Navbar.Brand>
                <Nav className="me-auto">
                    <Nav.Link as={Link} to="/">Home</Nav.Link>
                    <Nav.Link as={Link} to="/text-search">Text Search</Nav.Link>
                    <Nav.Link as={Link} to="/visual-search">Visual Search</Nav.Link>
                </Nav>
                </Container>
            </Navbar>

            <Routes>
                <Route path="/" element={<h2>Home</h2>} />
                <Route path="/text-search" element={<BM25 />} />
                <Route path="/visual-search" element={<VisualSearch />} />
            </Routes>
        </div>
    )
}

export default Navigation