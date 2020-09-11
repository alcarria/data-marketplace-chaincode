//
// Copyright (c) 2019 LG Electronics Inc.
// SPDX-License-Identifier: Apache-2.0
//

package resources

import (
	"encoding/json"
	"fmt"
        logger "github.com/sirupsen/logrus"
	"github.com/hyperledger/fabric-chaincode-go/shim"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/alcarria/data-marketplace-chaincode/utils"
)

type DataInfoSentToConsumer struct {
	Hash           Hash   `json:"hash"`
	DataContractID string `json:"dataContract"`
}

type DataReceivedByConsumer struct {
	DataContractID string `json:"dataContract"`
}

func SetDataInfoSentToConsumer(stub shim.ChaincodeStubInterface, dataInfo DataInfoSentToConsumer) pb.Response {
	logger.Info("entering-SetDataInfoSentToConsumer")
	defer logger.Info("exiting-SetDataInfoSentToConsumer")

	// ==== Check data attributes
	dataContract, err := checkAndGetAttributes(stub, dataInfo.DataContractID)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}

	// ==== Check if business already exists ====
	if dataContract.Extras.FileStatus != PROPOSAL {
		errorMsg := "data is either shipped or received or a stream"
		logger.Error(errorMsg)
		return shim.Error(errorMsg)
	}

	err = dataContract.SetFileStatus(stub, DATASHIPPED, dataInfo.Hash, true)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}

	consumerID, err := utils.GetAccountIDFromToken(fmt.Sprintf("%s-%s", ACCOUNT_DOCTYPE, dataContract.ConsumerID))
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	providerID, err := utils.GetAccountIDFromToken(fmt.Sprintf("%s-%s", ACCOUNT_DOCTYPE, dataContract.ProviderID))
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}

	dataContarctType, err := GetDataContractTypeStructState(stub, dataContract.DataContractTypeID)

	_, err = TransferFrom(stub, consumerID, providerID, dataContarctType.PriceType.Amount)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(nil)
}

func SetDataReceivedByConsumer(stub shim.ChaincodeStubInterface, dataReceived DataReceivedByConsumer) pb.Response {
	logger.Info("entering-SetDataReceivedByConsumer")
	defer logger.Info("exiting-SetDataReceivedByConsumer")

	// ==== Check data attributes
	dataContract, err := checkAndGetAttributes(stub, dataReceived.DataContractID)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}

	if dataContract.Extras.FileStatus != DATASHIPPED {
		errorMsg := "data is either proposal, received or a stream"
		logger.Error(errorMsg)
		return shim.Error(errorMsg)
	}

	err = dataContract.SetFileStatus(stub, DATARECEIVED, Hash{}, false)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}

	return shim.Success(nil)
}

func checkAndGetAttributes(stub shim.ChaincodeStubInterface, dataContractID string) (DataContract, error) {
	logger.Info("entering-checkAttribute-dataTransaction")
	defer logger.Info("exiting-checkAttributes-dataTransaction")

	dataContractAsBytes, err := GetDataContractState(stub, dataContractID)
	if err != nil {
		logger.Error(err.Error())
		return DataContract{}, err
	}
	dataContract := DataContract{}
	err = json.Unmarshal(dataContractAsBytes, &dataContract)
	if err != nil {
		logger.Error(err.Error())
		return DataContract{}, err
	}

	return dataContract, nil
}
