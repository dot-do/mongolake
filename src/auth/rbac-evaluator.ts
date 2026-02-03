/**
 * RBAC Evaluator
 *
 * Role-Based Access Control implementation for database/collection permissions.
 * Supports hierarchical roles with inheritance and wildcard patterns.
 */

import type {
  UserContext,
  RBACConfig,
  RoleDefinition,
  ResourcePermission,
  PermissionLevel,
  PermissionCheckResult,
} from './types.js';

// ============================================================================
// Permission Level Utilities
// ============================================================================

/** Permission level hierarchy (higher index = more permissions) */
const PERMISSION_HIERARCHY: PermissionLevel[] = ['none', 'read', 'write', 'admin'];

/**
 * Compare two permission levels.
 * Returns negative if a < b, 0 if equal, positive if a > b
 */
export function comparePermissionLevels(a: PermissionLevel, b: PermissionLevel): number {
  return PERMISSION_HIERARCHY.indexOf(a) - PERMISSION_HIERARCHY.indexOf(b);
}

/**
 * Get the higher of two permission levels
 */
export function maxPermissionLevel(a: PermissionLevel, b: PermissionLevel): PermissionLevel {
  return comparePermissionLevels(a, b) >= 0 ? a : b;
}

// ============================================================================
// RBAC Manager
// ============================================================================

/**
 * RBAC Manager for checking permissions
 */
export class RBACManager {
  private config: RBACConfig;
  private roleMap: Map<string, RoleDefinition>;

  constructor(config: RBACConfig) {
    this.config = config;
    this.roleMap = new Map();

    // Build role map for quick lookup
    for (const role of config.roles) {
      this.roleMap.set(role.name, role);
    }
  }

  /**
   * Get all permissions for a role, including inherited permissions
   */
  private getRolePermissions(roleName: string, visited: Set<string> = new Set()): ResourcePermission[] {
    // Prevent circular inheritance
    if (visited.has(roleName)) {
      return [];
    }
    visited.add(roleName);

    const role = this.roleMap.get(roleName);
    if (!role) {
      return [];
    }

    const permissions: ResourcePermission[] = [...role.permissions];

    // Add inherited permissions
    if (role.inheritsFrom) {
      for (const parentRole of role.inheritsFrom) {
        permissions.push(...this.getRolePermissions(parentRole, visited));
      }
    }

    return permissions;
  }

  /**
   * Check if a resource matches a permission pattern
   */
  private matchesResource(
    permission: ResourcePermission,
    database: string,
    collection?: string
  ): boolean {
    // Check database match
    if (permission.database !== '*' && permission.database !== database) {
      return false;
    }

    // If permission is database-level (no collection), it applies to all collections
    if (!permission.collection) {
      return true;
    }

    // If checking database-level access and permission has specific collection
    if (!collection) {
      return true;
    }

    // Check collection match
    return permission.collection === '*' || permission.collection === collection;
  }

  /**
   * Get the effective permission level for a user on a resource
   */
  getEffectivePermission(
    user: UserContext,
    database: string,
    collection?: string
  ): PermissionCheckResult {
    if (!this.config.enabled) {
      // RBAC disabled - all authenticated users have admin access
      return { allowed: true, effectivePermission: 'admin', reason: 'RBAC disabled' };
    }

    // Get all roles for the user
    const userRoles = user.roles.length > 0 ? user.roles : (this.config.defaultRole ? [this.config.defaultRole] : []);

    if (userRoles.length === 0) {
      return { allowed: false, effectivePermission: 'none', reason: 'User has no roles' };
    }

    // Find the highest permission level across all roles
    let highestPermission: PermissionLevel = 'none';
    let matchedRole: string | undefined;

    for (const roleName of userRoles) {
      const permissions = this.getRolePermissions(roleName);

      for (const permission of permissions) {
        if (this.matchesResource(permission, database, collection)) {
          if (comparePermissionLevels(permission.level, highestPermission) > 0) {
            highestPermission = permission.level;
            matchedRole = roleName;
          }
        }
      }
    }

    return {
      allowed: highestPermission !== 'none',
      effectivePermission: highestPermission,
      matchedRole,
    };
  }

  /**
   * Check if a user has at least a specific permission level on a resource
   */
  checkPermission(
    user: UserContext,
    database: string,
    collection: string | undefined,
    requiredLevel: PermissionLevel
  ): PermissionCheckResult {
    const effective = this.getEffectivePermission(user, database, collection);

    if (!effective.allowed) {
      return { ...effective, allowed: false };
    }

    const hasPermission = comparePermissionLevels(effective.effectivePermission, requiredLevel) >= 0;
    return {
      ...effective,
      allowed: hasPermission,
      reason: hasPermission ? undefined : `Requires ${requiredLevel} permission, has ${effective.effectivePermission}`,
    };
  }

  /**
   * Check read permission
   */
  canRead(user: UserContext, database: string, collection?: string): PermissionCheckResult {
    return this.checkPermission(user, database, collection, 'read');
  }

  /**
   * Check write permission
   */
  canWrite(user: UserContext, database: string, collection?: string): PermissionCheckResult {
    return this.checkPermission(user, database, collection, 'write');
  }

  /**
   * Check admin permission
   */
  canAdmin(user: UserContext, database: string, collection?: string): PermissionCheckResult {
    return this.checkPermission(user, database, collection, 'admin');
  }

  /**
   * List all databases/collections the user has access to
   */
  listAccessibleResources(user: UserContext): Array<{ database: string; collection?: string; level: PermissionLevel }> {
    if (!this.config.enabled) {
      return [{ database: '*', collection: '*', level: 'admin' }];
    }

    const userRoles = user.roles.length > 0 ? user.roles : (this.config.defaultRole ? [this.config.defaultRole] : []);
    const resources: Map<string, { database: string; collection?: string; level: PermissionLevel }> = new Map();

    for (const roleName of userRoles) {
      const permissions = this.getRolePermissions(roleName);

      for (const permission of permissions) {
        const key = `${permission.database}/${permission.collection ?? '*'}`;
        const existing = resources.get(key);

        if (!existing || comparePermissionLevels(permission.level, existing.level) > 0) {
          resources.set(key, {
            database: permission.database,
            collection: permission.collection,
            level: permission.level,
          });
        }
      }
    }

    return Array.from(resources.values());
  }
}
