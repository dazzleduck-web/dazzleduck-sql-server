import React from "react";
import { NavLink } from "react-router-dom";
import dazzleDuckLogo from "../assets/dd-logo.jpeg"

const Navbar = () => {
  return (
    <nav className="bg-[rgba(0,0,0,0.19)] text-black px-4 py-2 shadow-sm relative">
      <div className="flex justify-between items-center">
        {/* Logo */}
        <div className="h-auto w-auto flex-shrink-0">
          <NavLink to="/" className="flex items-center gap-3 font-bold">
            <img className="w-15 ml-10 rounded-full" src={dazzleDuckLogo} alt="Dazzle Duck" />
            <h1 className="text-gray-800 text-2xl font-mono">DazzleDuck</h1>
          </NavLink>
        </div>
      </div>
    </nav>
  );
};

export default Navbar;