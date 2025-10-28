import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import Logging from "./logging/Logging";
import Navbar from "./components/navbar";

function App() {
  return (
    <div>
      <Router>
      <Navbar />
        <Routes>
            <Route path="/" element={<Logging />} />
            <Route path="/logs" element={<Logging />} />
        </Routes>
      </Router>
    </div>
  );
}

export default App;
