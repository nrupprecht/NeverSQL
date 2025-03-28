#include <filesystem>
#include "neversql/utility/HexDump.h"

//! \brief Basic application to perform a hex-dump of a file. 
int main(int argc, char** argv) {

    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <file-path>" << std::endl;
        return 1;
    }
    auto filepath = std::filesystem::path(argv[1]);

    neversql::utility::HexDump(filepath, std::cout);

    return 0;
}