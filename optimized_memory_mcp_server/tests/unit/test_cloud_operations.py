"""Unit tests for cloud resource operations."""
import pytest
from datetime import datetime
import json
from ....exceptions import ResourceNotFoundError
from ....storage.sqlite.operations.cloud_ops import CloudResourceOperations

async def test_create_cloud_resources(connection_pool):
    """Test creating cloud resources."""
    cloud_ops = CloudResourceOperations(connection_pool)
    
    resources = [{
        "resource_id": "i-1234567890",
        "resource_type": "aws_instance",
        "region": "us-west-2",
        "account_id": "123456789012",
        "metadata": {"instance_type": "t2.micro"},
        "entity_name": "test_entity_1",
        "tags": {"Name": "test-instance"}
    }]
    
    created = await cloud_ops.create_resources(resources)
    assert len(created) == 1
    assert created[0]["resource_id"] == "i-1234567890"
    
    # Verify in database
    async with connection_pool.get_connection() as conn:
        cursor = await conn.execute(
            "SELECT * FROM cloud_resources WHERE resource_id = ?",
            ("i-1234567890",)
        )
        row = await cursor.fetchone()
        assert row is not None
        assert row["resource_type"] == "aws_instance"
        assert json.loads(row["metadata"])["instance_type"] == "t2.micro"

async def test_create_cloud_resources_batch(connection_pool):
    """Test batch creation of cloud resources."""
    cloud_ops = CloudResourceOperations(connection_pool)
    
    resources = [
        {
            "resource_id": f"vpc-{i}",
            "resource_type": "aws_vpc",
            "region": "us-west-2",
            "account_id": "123456789012",
            "metadata": {"cidr_block": f"10.0.{i}.0/24"},
            "tags": {"Name": f"test-vpc-{i}"}
        }
        for i in range(5)
    ]
    
    created = await cloud_ops.create_resources(resources, batch_size=2)
    assert len(created) == 5
    
    # Verify all resources were created
    async with connection_pool.get_connection() as conn:
        cursor = await conn.execute(
            "SELECT COUNT(*) as count FROM cloud_resources WHERE resource_type = ?",
            ("aws_vpc",)
        )
        row = await cursor.fetchone()
        assert row["count"] == 5

async def test_update_cloud_resource(connection_pool):
    """Test updating cloud resource metadata."""
    cloud_ops = CloudResourceOperations(connection_pool)
    
    # Create initial resource
    resource = {
        "resource_id": "rds-123",
        "resource_type": "aws_rds",
        "region": "us-west-2",
        "account_id": "123456789012",
        "metadata": {"instance_class": "db.t3.micro"},
        "tags": {"Environment": "test"}
    }
    await cloud_ops.create_resources([resource])
    
    # Update metadata
    updated_metadata = {"instance_class": "db.t3.small"}
    await cloud_ops.update_resource_metadata("rds-123", updated_metadata)
    
    # Verify update
    async with connection_pool.get_connection() as conn:
        cursor = await conn.execute(
            "SELECT metadata FROM cloud_resources WHERE resource_id = ?",
            ("rds-123",)
        )
        row = await cursor.fetchone()
        assert json.loads(row["metadata"])["instance_class"] == "db.t3.small"

async def test_delete_cloud_resources(connection_pool):
    """Test deleting cloud resources."""
    cloud_ops = CloudResourceOperations(connection_pool)
    
    # Create resources to delete
    resources = [
        {
            "resource_id": "delete-test-1",
            "resource_type": "aws_instance",
            "region": "us-west-2"
        },
        {
            "resource_id": "delete-test-2",
            "resource_type": "aws_instance",
            "region": "us-west-2"
        }
    ]
    await cloud_ops.create_resources(resources)
    
    # Delete resources
    await cloud_ops.delete_resources(["delete-test-1", "delete-test-2"])
    
    # Verify deletion
    async with connection_pool.get_connection() as conn:
        cursor = await conn.execute(
            "SELECT COUNT(*) as count FROM cloud_resources WHERE resource_id IN (?, ?)",
            ("delete-test-1", "delete-test-2")
        )
        row = await cursor.fetchone()
        assert row["count"] == 0

async def test_get_resources_by_type(connection_pool):
    """Test retrieving resources by type."""
    cloud_ops = CloudResourceOperations(connection_pool)
    
    # Create resources of different types
    resources = [
        {
            "resource_id": "lambda-1",
            "resource_type": "aws_lambda",
            "region": "us-west-2"
        },
        {
            "resource_id": "lambda-2",
            "resource_type": "aws_lambda",
            "region": "us-west-2"
        },
        {
            "resource_id": "s3-1",
            "resource_type": "aws_s3",
            "region": "us-west-2"
        }
    ]
    await cloud_ops.create_resources(resources)
    
    # Get lambda resources
    lambda_resources = await cloud_ops.get_resources_by_type("aws_lambda")
    assert len(lambda_resources) == 2
    assert all(r["resource_type"] == "aws_lambda" for r in lambda_resources)

async def test_get_resources_by_region(connection_pool):
    """Test retrieving resources by region."""
    cloud_ops = CloudResourceOperations(connection_pool)
    
    # Create resources in different regions
    resources = [
        {
            "resource_id": "east-1",
            "resource_type": "aws_instance",
            "region": "us-east-1"
        },
        {
            "resource_id": "west-1",
            "resource_type": "aws_instance",
            "region": "us-west-2"
        }
    ]
    await cloud_ops.create_resources(resources)
    
    # Get resources by region
    east_resources = await cloud_ops.get_resources_by_region("us-east-1")
    assert len(east_resources) == 1
    assert east_resources[0]["resource_id"] == "east-1"

async def test_update_resource_tags(connection_pool):
    """Test updating resource tags."""
    cloud_ops = CloudResourceOperations(connection_pool)
    
    # Create resource with initial tags
    resource = {
        "resource_id": "tag-test",
        "resource_type": "aws_instance",
        "region": "us-west-2",
        "tags": {"Environment": "dev"}
    }
    await cloud_ops.create_resources([resource])
    
    # Update tags
    new_tags = {"Environment": "prod", "Team": "engineering"}
    await cloud_ops.update_resource_tags("tag-test", new_tags)
    
    # Verify tags
    async with connection_pool.get_connection() as conn:
        cursor = await conn.execute(
            "SELECT tags FROM cloud_resources WHERE resource_id = ?",
            ("tag-test",)
        )
        row = await cursor.fetchone()
        stored_tags = json.loads(row["tags"])
        assert stored_tags["Environment"] == "prod"
        assert stored_tags["Team"] == "engineering"

async def test_get_resources_by_entity(connection_pool):
    """Test retrieving resources associated with an entity."""
    cloud_ops = CloudResourceOperations(connection_pool)
    
    # Create resources linked to entity
    resources = [
        {
            "resource_id": "entity-res-1",
            "resource_type": "aws_instance",
            "region": "us-west-2",
            "entity_name": "test_entity"
        },
        {
            "resource_id": "entity-res-2",
            "resource_type": "aws_rds",
            "region": "us-west-2",
            "entity_name": "test_entity"
        }
    ]
    await cloud_ops.create_resources(resources)
    
    # Get resources for entity
    entity_resources = await cloud_ops.get_resources_by_entity("test_entity")
    assert len(entity_resources) == 2
    assert all(r["entity_name"] == "test_entity" for r in entity_resources)

async def test_nonexistent_resource(connection_pool):
    """Test operations on non-existent resources raise appropriate errors."""
    cloud_ops = CloudResourceOperations(connection_pool)
    
    with pytest.raises(ResourceNotFoundError):
        await cloud_ops.update_resource_metadata("nonexistent", {})
    
    with pytest.raises(ResourceNotFoundError):
        await cloud_ops.update_resource_tags("nonexistent", {})

async def test_resource_state_transitions(connection_pool):
    """Test resource state transitions."""
    cloud_ops = CloudResourceOperations(connection_pool)
    
    # Create resource in initial state
    resource = {
        "resource_id": "state-test",
        "resource_type": "aws_instance",
        "region": "us-west-2",
        "metadata": {"state": "pending"}
    }
    await cloud_ops.create_resources([resource])
    
    # Update state transitions
    states = ["running", "stopping", "stopped"]
    for state in states:
        await cloud_ops.update_resource_metadata("state-test", {"state": state})
        
        # Verify state
        async with connection_pool.get_connection() as conn:
            cursor = await conn.execute(
                "SELECT metadata FROM cloud_resources WHERE resource_id = ?",
                ("state-test",)
            )
            row = await cursor.fetchone()
            assert json.loads(row["metadata"])["state"] == state

async def test_resource_relationships(connection_pool):
    """Test tracking relationships between resources."""
    cloud_ops = CloudResourceOperations(connection_pool)
    
    # Create related resources
    resources = [
        {
            "resource_id": "vpc-1",
            "resource_type": "aws_vpc",
            "region": "us-west-2",
            "metadata": {"cidr": "10.0.0.0/16"}
        },
        {
            "resource_id": "subnet-1",
            "resource_type": "aws_subnet",
            "region": "us-west-2",
            "metadata": {"vpc_id": "vpc-1", "cidr": "10.0.1.0/24"}
        }
    ]
    await cloud_ops.create_resources(resources)
    
    # Verify relationship tracking
    related = await cloud_ops.get_related_resources("vpc-1")
    assert len(related) == 1
    assert related[0]["resource_id"] == "subnet-1"
    assert related[0]["metadata"]["vpc_id"] == "vpc-1"

async def test_invalid_resource_type(connection_pool):
    """Test handling of invalid resource types."""
    cloud_ops = CloudResourceOperations(connection_pool)
    
    with pytest.raises(ValueError, match="Invalid resource type"):
        await cloud_ops.create_resources([{
            "resource_id": "invalid-1",
            "resource_type": "invalid_type",
            "region": "us-west-2"
        }])

async def test_paginated_results(connection_pool):
    """Test pagination for large result sets."""
    cloud_ops = CloudResourceOperations(connection_pool)
    
    # Create many resources
    resources = [
        {
            "resource_id": f"page-test-{i}",
            "resource_type": "aws_instance",
            "region": "us-west-2"
        }
        for i in range(25)
    ]
    await cloud_ops.create_resources(resources)
    
    # Test pagination
    page1 = await cloud_ops.get_resources_by_type("aws_instance", page_size=10, page=1)
    page2 = await cloud_ops.get_resources_by_type("aws_instance", page_size=10, page=2)
    page3 = await cloud_ops.get_resources_by_type("aws_instance", page_size=10, page=3)
    
    assert len(page1) == 10
    assert len(page2) == 10
    assert len(page3) == 5
    
    # Verify no duplicates
    all_ids = [r["resource_id"] for r in page1 + page2 + page3]
    assert len(all_ids) == len(set(all_ids))

async def test_metadata_validation(connection_pool):
    """Test resource metadata validation."""
    cloud_ops = CloudResourceOperations(connection_pool)
    
    # Test invalid metadata types
    with pytest.raises(ValueError, match="Invalid metadata"):
        await cloud_ops.create_resources([{
            "resource_id": "metadata-test",
            "resource_type": "aws_instance",
            "region": "us-west-2",
            "metadata": "invalid"  # Should be dict
        }])
    
    # Test required metadata fields
    with pytest.raises(ValueError, match="Missing required metadata"):
        await cloud_ops.create_resources([{
            "resource_id": "metadata-test",
            "resource_type": "aws_rds",
            "region": "us-west-2",
            "metadata": {}  # Missing required fields for RDS
        }])
    
    # Test metadata field validation
    with pytest.raises(ValueError, match="Invalid instance type"):
        await cloud_ops.create_resources([{
            "resource_id": "metadata-test",
            "resource_type": "aws_instance",
            "region": "us-west-2",
            "metadata": {"instance_type": "invalid.type"}
        }])
