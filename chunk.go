package graphsplit

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("graphsplit")

type GraphBuildCallback interface {
	OnSuccess(node ipld.Node, graphName, fsDetail string)
	OnError(error)
}

type commPCallback struct {
	carDir     string
	rename     bool
	addPadding bool

	*carDesc
}

func (cc *commPCallback) OnSuccess(node ipld.Node, graphName, fsDetail string) {
	fmt.Println("xxxxx")
	commpStartTime := time.Now()
	carfilepath := path.Join(cc.carDir, node.Cid().String()+".car")
	cpRes, err := CalcCommP(context.TODO(), carfilepath, cc.rename, cc.addPadding)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("calculation of pieceCID completed, time elapsed: %s", time.Now().Sub(commpStartTime))
	// Add node inof to manifest.csv
	manifestPath := path.Join(cc.carDir, "manifest.csv")
	_, err = os.Stat(manifestPath)
	if err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}
	var isCreateAction bool
	if err != nil && os.IsNotExist(err) {
		isCreateAction = true
	}
	f, err := os.OpenFile(manifestPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	csvWriter := csv.NewWriter(f)
	csvWriter.UseCRLF = true
	defer csvWriter.Flush()
	if isCreateAction {
		csvWriter.Write([]string{
			"playload_cid", "filename", "piece_cid", "payload_size", "piece_size", "detail",
		})
	}

	if err := csvWriter.Write([]string{
		node.Cid().String(), graphName, cpRes.Root.String(), strconv.FormatInt(cpRes.PayloadSize, 10), strconv.FormatUint(uint64(cpRes.Size), 10), fsDetail,
	}); err != nil {
		log.Fatal(err)
	}

	// record car file desc if needed
	if cc.carDesc != nil {
		cc.recordFilesDesc(carfilepath, node.Cid().String(), graphName, cpRes.Root.String(), cpRes.PayloadSize, int64(cpRes.Size), fsDetail)
	}
}

func (cc *commPCallback) OnError(err error) {
	log.Fatal(err)
}

type csvCallback struct {
	carDir string
}

func (cc *csvCallback) OnSuccess(node ipld.Node, graphName, fsDetail string) {
	// Add node inof to manifest.csv
	manifestPath := path.Join(cc.carDir, "manifest.csv")
	_, err := os.Stat(manifestPath)
	if err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}
	var isCreateAction bool
	if err != nil && os.IsNotExist(err) {
		isCreateAction = true
	}
	f, err := os.OpenFile(manifestPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	if isCreateAction {
		if _, err := f.Write([]byte("playload_cid,filename,detail\n")); err != nil {
			log.Fatal(err)
		}
	}
	if _, err := f.Write([]byte(fmt.Sprintf("%s,%s,%s\n", node.Cid(), graphName, fsDetail))); err != nil {
		log.Fatal(err)
	}
}

func (cc *csvCallback) OnError(err error) {
	log.Fatal(err)
}

type errCallback struct{}

func (cc *errCallback) OnSuccess(ipld.Node, string, string) {}
func (cc *errCallback) OnError(err error) {
	log.Fatal(err)
}

func CommPCallbackWithCarDesc(carDir string, rename, addPadding bool, carDesc *carDesc) GraphBuildCallback {
	return &commPCallback{carDir: carDir, rename: rename, addPadding: addPadding, carDesc: carDesc}
}

func CommPCallback(carDir string, rename, addPadding bool) GraphBuildCallback {
	return &commPCallback{carDir: carDir, rename: rename, addPadding: addPadding}
}

func CSVCallback(carDir string) GraphBuildCallback {
	return &csvCallback{carDir: carDir}
}
func ErrCallback() GraphBuildCallback {
	return &errCallback{}
}

func Chunk(ctx context.Context, sliceSize int64, parentPath, targetPath, carDir, graphName string, parallel int, cb GraphBuildCallback, padding ...bool) error {
	if parentPath == "" {
		parentPath = targetPath
		if finfo, err := os.Stat(targetPath); err == nil && !finfo.IsDir() {
			index := strings.LastIndex(targetPath, "/")
			parentPath = targetPath[:index]
		}
	}
	return ChunkMulti(ctx, sliceSize, parentPath, []string{targetPath}, carDir, graphName, parallel, cb, padding...)
}

func ChunkMulti(ctx context.Context, sliceSize int64, parentPath string, targetsPath []string, carDir, graphName string, parallel int, cb GraphBuildCallback, padding ...bool) error {
	var cumuSize int64 = 0
	graphSliceCount := 0
	graphFiles := make([]Finfo, 0)
	if sliceSize == 0 {
		return xerrors.Errorf("Unexpected! Slice size has been set as 0")
	}
	if parallel <= 0 {
		return xerrors.Errorf("Unexpected! Parallel has to be greater than 0")
	}
	if parentPath == "" {
		return errors.New("Unexpected! ParentPath not be empty")
	}
	if len(targetsPath) == 0 {
		return errors.New("Unexpected! TargetsPath size has to be greater than 0")
	}

	args := targetsPath
	sliceTotal, err := GetGraphCount(args, sliceSize)
	if err != nil {
		return fmt.Errorf("GetGraphCount error :%w", err)
	}
	if sliceTotal == 0 {
		log.Warn("Empty folder or file!")
		return nil
	}

	files := GetFileListAsync(args)
	for item := range files {
		fileSize := item.Info.Size()
		switch {
		case cumuSize+fileSize < sliceSize:
			cumuSize += fileSize
			graphFiles = append(graphFiles, item)
		case cumuSize+fileSize == sliceSize:
			cumuSize += fileSize
			graphFiles = append(graphFiles, item)
			// todo build ipld from graphFiles
			BuildIpldGraph(ctx, graphFiles, GenGraphName(graphName, graphSliceCount, sliceTotal), parentPath, carDir, parallel, cb)
			fmt.Printf("cumu-size: %d\n", cumuSize)
			fmt.Print(GenGraphName(graphName, graphSliceCount, sliceTotal))
			fmt.Printf("=================\n")
			cumuSize = 0
			graphFiles = make([]Finfo, 0)
			graphSliceCount++
		case cumuSize+fileSize > sliceSize:
			fileSliceCount := 0
			// need to split item to fit graph slice
			//
			// first cut
			firstCut := sliceSize - cumuSize
			var seekStart int64 = 0
			var seekEnd int64 = seekStart + firstCut - 1
			fmt.Printf("first cut %d, seek start at %d, end at %d", firstCut, seekStart, seekEnd)
			fmt.Printf("----------------\n")
			graphFiles = append(graphFiles, Finfo{
				Path:      item.Path,
				Name:      fmt.Sprintf("%s.%08d", item.Info.Name(), fileSliceCount),
				Info:      item.Info,
				SeekStart: seekStart,
				SeekEnd:   seekEnd,
			})
			fileSliceCount++
			// todo build ipld from graphFiles
			BuildIpldGraph(ctx, graphFiles, GenGraphName(graphName, graphSliceCount, sliceTotal), parentPath, carDir, parallel, cb)
			fmt.Printf("cumu-size: %d\n", cumuSize+firstCut)
			fmt.Print(GenGraphName(graphName, graphSliceCount, sliceTotal))
			fmt.Printf("=================\n")
			cumuSize = 0
			graphFiles = make([]Finfo, 0)
			graphSliceCount++
			for seekEnd < fileSize-1 {
				seekStart = seekEnd + 1
				seekEnd = seekStart + sliceSize - 1
				if seekEnd >= fileSize-1 {
					seekEnd = fileSize - 1
				}
				fmt.Printf("following cut %d, seek start at %d, end at %d", seekEnd-seekStart+1, seekStart, seekEnd)
				fmt.Printf("----------------\n")
				cumuSize += seekEnd - seekStart + 1
				graphFiles = append(graphFiles, Finfo{
					Path:      item.Path,
					Name:      fmt.Sprintf("%s.%08d", item.Info.Name(), fileSliceCount),
					Info:      item.Info,
					SeekStart: seekStart,
					SeekEnd:   seekEnd,
				})
				fileSliceCount++
				if seekEnd-seekStart == sliceSize-1 {
					// todo build ipld from graphFiles
					BuildIpldGraph(ctx, graphFiles, GenGraphName(graphName, graphSliceCount, sliceTotal), parentPath, carDir, parallel, cb)
					fmt.Printf("cumu-size: %d\n", sliceSize)
					fmt.Print(GenGraphName(graphName, graphSliceCount, sliceTotal))
					fmt.Printf("=================\n")
					cumuSize = 0
					graphFiles = make([]Finfo, 0)
					graphSliceCount++
				}
			}
		}
	}
	if cumuSize > 0 {
		// check padding
		if len(padding) > 0 && padding[0] {
			if f, err := generateRandomPaddingFile(parentPath, sliceSize-cumuSize); err != nil {
				log.Error("generate random padding file failed :", err)
			} else {
				cumuSize = sliceSize
				graphFiles = append(graphFiles, *f)
				defer os.Remove(f.Path) // remove temp file after car builded
			}
		}

		// todo build ipld from graphFiles
		BuildIpldGraph(ctx, graphFiles, GenGraphName(graphName, graphSliceCount, sliceTotal), parentPath, carDir, parallel, cb)
		fmt.Printf("cumu-size: %d\n", cumuSize)
		fmt.Print(GenGraphName(graphName, graphSliceCount, sliceTotal))
		fmt.Printf("=================\n")
	}

	// write car file desc to json file if needed
	if commp, ok := cb.(*commPCallback); ok && commp.carDesc != nil {
		if err = commp.writeFileDescsToJsonFile(carDir); err != nil {
			log.Error(err)
			return err
		}
	}
	return nil
}

const carPaddingFileName = "___car___.placeholder"

func generateRandomPaddingFile(path string, size int64) (f *Finfo, err error) {
	filePath := path + "/" + carPaddingFileName
	cmd := exec.Command("dd", "if=/dev/random", "of="+filePath, fmt.Sprintf("bs=%d", size), "count=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Error(string(out))
		return
	}
	finfo, err := os.Stat(filePath)
	if err != nil {
		log.Error(err)
		return
	}
	return &Finfo{
		Path: filePath,
		Name: finfo.Name(),
		Info: finfo,
	}, nil
}

func NewCarDesc(carMd5 bool, inputDir string, targetsPath ...string) *carDesc {
	wholeDir := false
	if len(targetsPath) == 1 {
		if fi, err := os.Stat(targetsPath[0]); err == nil && fi.IsDir() {
			wholeDir = true
		}
	}
	return &carDesc{
		carMd5:   carMd5,
		inputDir: inputDir,
		wholeDir: wholeDir,
	}
}

type carDesc struct {
	carMd5    bool
	wholeDir  bool
	inputDir  string
	fileDescs []*FileDesc
}

func (cd *carDesc) recordFilesDesc(carPath string, payloadCid string, filename string, pieceCid string, payloadSize, pieceSize int64, detail string) error {
	fd := FileDesc{
		PayloadCid:  payloadCid,
		PieceCid:    pieceCid,
		CarFileSize: int64(payloadSize),
		CarFileName: payloadCid + ".car",
		CarFilePath: carPath,
	}
	fd.CarFileUrl = fd.CarFileName

	var node fsNode
	if err := json.Unmarshal([]byte(detail), &node); err != nil {
		log.Error("Failed to parse: ", detail)
		return err
	}

	if cd.wholeDir {
		fd.SourceFileName = filepath.Base(cd.inputDir)
		fd.SourceFilePath = cd.inputDir
		for _, link := range node.Link {
			fd.SourceFileSize = fd.SourceFileSize + int64(link.Size)
		}
	} else {
		fd.SourceFileName = node.Link[0].Name
		fd.SourceFilePath = filepath.Join(cd.inputDir, fd.SourceFileName)
		fd.SourceFileSize = int64(node.Link[0].Size)
	}

	if cd.carMd5 {
		if _, err := os.Stat(fd.SourceFilePath); err == nil {
			srcFileMd5, err := MD5sum(fd.SourceFilePath)
			if err != nil {
				log.Error(err)
				return err
			}
			fd.SourceFileMd5 = srcFileMd5
		}

		carFileMd5, err := MD5sum(fd.CarFilePath)
		if err != nil {
			log.Error(err)
			return err
		}
		fd.CarFileMd5 = carFileMd5
	}
	cd.fileDescs = append(cd.fileDescs, &fd)
	return nil
}

func (cd *carDesc) writeFileDescsToJsonFile(carDir string) error {
	fileName := filepath.Base(carDir) + ".car.json"
	jsonFilePath := filepath.Join(carDir, fileName)
	content, err := json.MarshalIndent(cd.fileDescs, "", " ")
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(jsonFilePath, content, 0644)
	if err != nil {
		return err
	}

	log.Info("Metadata json file generated: ", jsonFilePath)
	return nil
}

type FileDesc struct {
	Uuid           string
	SourceFileName string
	SourceFilePath string
	SourceFileMd5  string
	SourceFileSize int64
	CarFileName    string
	CarFilePath    string
	CarFileMd5     string
	CarFileUrl     string
	CarFileSize    int64
	PayloadCid     string
	PieceCid       string
	StartEpoch     *int64
	SourceId       *int
	Deals          []*DealInfo
}

type DealInfo struct {
	DealCid    string
	MinerFid   string
	StartEpoch int
	Cost       string
}
